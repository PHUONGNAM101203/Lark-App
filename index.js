require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');
const { Readable } = require('stream'); // Thêm để xử lý stream

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

// Hàm hỗ trợ đọc Stream từ Lark thành Buffer hoàn chỉnh
async function streamToBuffer(stream) {
    if (Buffer.isBuffer(stream)) return stream;
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
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

                    // BƯỚC 1: Tải resource từ Lark
                    const response = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: file_key },
                        params: { type: 'file' }
                    });

                    // BƯỚC 2: Chuyển đổi Stream sang Buffer chắc chắn có dữ liệu
                    const fileBuffer = await streamToBuffer(response);
                    
                    // BƯỚC 3: Chuyển Buffer sang String và loại bỏ ký tự lạ (BOM)
                    let csvString = fileBuffer.toString('utf-8');
                    csvString = csvString.replace(/^\uFEFF/, ''); // Xóa BOM nếu có

                    if (!csvString || csvString.trim().length === 0) {
                        throw new Error("Nội dung file sau khi tải về bị rỗng.");
                    }

                    // BƯỚC 4: Parse CSV với cấu hình mạnh hơn
                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: 'greedy', // Bỏ qua tất cả dòng trống/trắng
                        dynamicTyping: true,
                        transformHeader: (h) => h.trim() // Xóa khoảng trắng ở tiêu đề
                    });

                    console.log(`📊 Đã parse thành công: ${parsed.data.length} dòng.`);

                    // BƯỚC 5: Lưu vào MongoDB
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString,
                        totalRows: parsed.data.length,
                        parsedData: parsed.data
                    });
                    const savedDoc = await newFileEntry.save();

                    // BƯỚC 6: Trả lời Card
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { 
                                    title: { tag: 'plain_text', content: '✅ Đã Nhập Kho Dữ Liệu' }, 
                                    template: "green" 
                                },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng thực tế:** ${parsed.data.length}\n🗄️ **ID bản ghi:** \`${savedDoc._id}\`` } },
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
            // Thông báo lỗi cho người dùng qua Lark
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi: ${error.message}` }) }
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
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa khỏi DB!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi khi xóa.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;