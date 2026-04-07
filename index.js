require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');

const app = express();
app.use(express.json());

// ==========================================
// 1. KẾT NỐI MONGODB
// ==========================================
let isConnected = false;
async function connectDB() {
    if (isConnected) return;
    try {
        if (mongoose.connections[0].readyState) {
            isConnected = true;
            return;
        }
        await mongoose.connect(process.env.MONGODB_URI);
        isConnected = true;
        console.log('✅ MongoDB Connected');
    } catch (err) {
        console.error('❌ MongoDB Connection Error:', err);
    }
}

const CsvVaultSchema = new mongoose.Schema({
    fileName: String,
    fileContentRaw: String,
    totalRows: Number,
    parsedData: [mongoose.Schema.Types.Mixed],
    importedAt: { type: Date, default: Date.now }
});

const CsvVault = mongoose.models.CsvVault || mongoose.model('CsvVault', CsvVaultSchema, 'csv_storage');

const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// ✅ HÀM 4.0: BẮT ĐÚNG ARRAYBUFFER, MỌI ĐỊNH DẠNG TỪ LARK
async function extractFileContent(response) {
    if (!response) throw new Error("Lark không trả về dữ liệu.");

    if (response.code && response.code !== 0) {
        throw new Error(`Lark từ chối (Mã ${response.code}): ${response.msg}`);
    }

    // 1. Tìm đúng túi chứa dữ liệu (Lark bọc trong .data hoặc .body)
    let target = response;
    if (response.data) target = response.data;
    else if (response.body) target = response.body;

    // 2. Nếu là Buffer chuẩn
    if (Buffer.isBuffer(target)) return target;

    // 3. Nếu là ArrayBuffer hoặc Uint8Array (Chính là thủ phạm gây lỗi vừa rồi)
    if (target instanceof ArrayBuffer || Object.prototype.toString.call(target) === '[object ArrayBuffer]') {
        return Buffer.from(target);
    }
    if (target.buffer && target.buffer instanceof ArrayBuffer) {
        return Buffer.from(target);
    }

    // 4. Nếu là Node.js Stream
    if (typeof target.on === 'function') {
        return new Promise((resolve, reject) => {
            const chunks = [];
            target.on('data', chunk => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
            target.on('error', reject);
            target.on('end', () => resolve(Buffer.concat(chunks)));
        });
    }

    // 5. Nếu là Web Stream (Fetch API)
    if (typeof target.getReader === 'function') {
        const reader = target.getReader();
        const chunks = [];
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (value) chunks.push(Buffer.isBuffer(value) ? value : Buffer.from(value));
        }
        return Buffer.concat(chunks);
    }

    // Cứu cánh cuối cùng
    try {
        return Buffer.from(target);
    } catch (e) {
        throw new Error("Không thể dịch định dạng này. Kiểu dữ liệu: " + typeof target);
    }
}

// =====================================================================
// 2. WEBHOOK CHÍNH
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        try {
            if (message.message_type === 'file') {
                const { file_name, file_key } = JSON.parse(message.content);

                if (file_name.toLowerCase().endsWith('.csv')) {
                    console.log(`📂 Đang tải file: ${file_name}`);

                    // Kéo dữ liệu từ Lark
                    const response = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: file_key },
                        params: { type: 'file' }
                    });

                    // Chuyển toàn bộ thành Buffer
                    const fileBuffer = await extractFileContent(response);
                    
                    // Biến đổi thành Text
                    let csvString = fileBuffer.toString('utf-8');
                    csvString = csvString.replace(/^\uFEFF/, ''); // Xóa ký tự rác (BOM)

                    if (!csvString.trim()) throw new Error("Nội dung file rỗng.");

                    // Bóc tách CSV
                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: 'greedy',
                        dynamicTyping: true,
                        transformHeader: (h) => h.trim()
                    });

                    // Lưu Database
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString,
                        totalRows: parsed.data.length,
                        parsedData: parsed.data
                    });
                    const savedDoc = await newFileEntry.save();

                    // Gửi Card Báo cáo
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ Đã Nhập DB Thành Công' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng:** ${parsed.data.length}\n🗄️ **ID Bản ghi:** \`${savedDoc._id}\`` } },
                                    {
                                        tag: 'action',
                                        actions: [{
                                            tag: 'button',
                                            text: { tag: 'plain_text', content: '🗑️ Xóa bản ghi' },
                                            type: 'danger',
                                            value: { action: 'delete_file', docId: savedDoc._id }
                                        }]
                                    }
                                ]
                            })
                        }
                    });
                }
            }
        } catch (error) {
            console.error("❌ Lỗi:", error.message);
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi xử lý: ${error.message}` }) }
            });
        }
    }
    return res.status(200).json({ success: true });
});

app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa dữ liệu khỏi MongoDB!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi xóa bản ghi.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;