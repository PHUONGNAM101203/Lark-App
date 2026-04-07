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

// ✅ HÀM MỚI: Đọc Stream tương thích 100% (Sửa lỗi "not async iterable")
function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        if (Buffer.isBuffer(stream)) return resolve(stream);
        
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', (err) => reject(err));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
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

                    // 1. Tải resource từ Lark
                    const response = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: file_key },
                        params: { type: 'file' }
                    });

                    // 2. Sử dụng hàm Promise để đọc Stream (Khắc phục lỗi)
                    const fileBuffer = await streamToBuffer(response);
                    
                    // 3. Xử lý nội dung
                    let csvString = fileBuffer.toString('utf-8');
                    csvString = csvString.replace(/^\uFEFF/, ''); // Xóa BOM

                    if (!csvString.trim()) throw new Error("File rỗng.");

                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: 'greedy',
                        dynamicTyping: true,
                        transformHeader: (h) => h.trim()
                    });

                    // 4. Lưu MongoDB
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString,
                        totalRows: parsed.data.length,
                        parsedData: parsed.data
                    });
                    const savedDoc = await newFileEntry.save();

                    // 5. Trả lời Card
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ Đã Nhập Kho Dữ Liệu' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng:** ${parsed.data.length}\n🗄️ **ID:** \`${savedDoc._id}\`` } },
                                    {
                                        tag: 'action',
                                        actions: [{
                                            tag: 'button',
                                            text: { tag: 'plain_text', content: '🗑️ Xóa dữ liệu' },
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
            console.error("❌ Lỗi xử lý:", error.message);
            // Gửi thông báo lỗi cụ thể về Lark cho dễ debug
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi hệ thống: ${error.message}` }) }
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
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi xóa.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;