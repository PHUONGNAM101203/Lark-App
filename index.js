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

// ✅ HÀM 3.0: Bắt lỗi thông minh & Xử lý mọi loại định dạng từ Lark
async function extractFileContent(response) {
    if (!response) throw new Error("Lark không trả về dữ liệu.");

    // 1. Nếu Lark trả về lỗi dạng JSON (Thường là lỗi thiếu quyền)
    if (response.code && response.code !== 0) {
        throw new Error(`Lark từ chối (Mã ${response.code}): ${response.msg}`);
    }

    // 2. Nếu trả về File dạng Buffer
    if (Buffer.isBuffer(response)) return response;

    // 3. Nếu dữ liệu bị bọc bên trong trường response.data
    if (response.data && Buffer.isBuffer(response.data)) return response.data;

    // 4. Nếu trả về dạng Stream
    if (typeof response.on === 'function') {
        return new Promise((resolve, reject) => {
            const chunks = [];
            response.on('data', (chunk) => chunks.push(chunk));
            response.on('error', (err) => reject(err));
            response.on('end', () => resolve(Buffer.concat(chunks)));
        });
    }

    // 5. Nếu trả về một Object lạ hoắc, báo lỗi thẳng ra màn hình để biết đường sửa
    throw new Error("Lark trả về dữ liệu không phải File: " + JSON.stringify(response));
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
                    console.log(`📂 Đang yêu cầu tải file: ${file_name}`);

                    // Yêu cầu Lark cho tải file
                    const response = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: file_key },
                        params: { type: 'file' }
                    });

                    // Ép dữ liệu qua hàm thông minh để lấy File (hoặc lấy Lỗi)
                    const fileBuffer = await extractFileContent(response);
                    
                    let csvString = fileBuffer.toString('utf-8');
                    csvString = csvString.replace(/^\uFEFF/, ''); // Xóa BOM

                    if (!csvString.trim()) throw new Error("Nội dung file rỗng.");

                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: 'greedy',
                        dynamicTyping: true,
                        transformHeader: (h) => h.trim()
                    });

                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString,
                        totalRows: parsed.data.length,
                        parsedData: parsed.data
                    });
                    const savedDoc = await newFileEntry.save();

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ Đã Nhập Kho DB' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng:** ${parsed.data.length}` } },
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
            // Gửi tin nhắn chứa ĐÚNG LỖI mà Lark trả về cho bạn xem
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi hệ thống: ${error.message}` }) }
            });
        }
    }
    return res.status(200).json({ success: true });
});

app.post('/webhook/card', async (req, res) => {
    // ... Giữ nguyên như cũ
    const data = req.body || {};
    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi xóa.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;