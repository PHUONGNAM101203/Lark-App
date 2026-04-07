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

// ✅ HÀM VẠN NĂNG: Xử lý cả Buffer lẫn Stream
async function ensureBuffer(data) {
    if (!data) return Buffer.alloc(0);
    
    // Nếu Lark đã trả về Buffer rồi thì dùng luôn
    if (Buffer.isBuffer(data)) return data;

    // Nếu là Stream (có hàm .on) thì mới thực hiện đọc stream
    if (typeof data.on === 'function') {
        return new Promise((resolve, reject) => {
            const chunks = [];
            data.on('data', (chunk) => chunks.push(chunk));
            data.on('error', (err) => reject(err));
            data.on('end', () => resolve(Buffer.concat(chunks)));
        });
    }

    // Nếu là chuỗi hoặc kiểu khác, ép về Buffer
    return Buffer.from(data);
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
                    console.log(`📂 Đang xử lý file: ${file_name}`);

                    // 1. Tải tài nguyên từ Lark
                    const response = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: file_key },
                        params: { type: 'file' }
                    });

                    // 2. Đảm bảo lấy được Buffer (Dùng hàm ensureBuffer mới)
                    const fileBuffer = await ensureBuffer(response);
                    
                    // 3. Xử lý nội dung văn bản
                    let csvString = fileBuffer.toString('utf-8');
                    csvString = csvString.replace(/^\uFEFF/, ''); // Xóa ký tự BOM nếu có

                    if (!csvString.trim()) {
                        throw new Error("Nội dung file rỗng hoặc không đọc được.");
                    }

                    // 4. Parse CSV
                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: 'greedy',
                        dynamicTyping: true,
                        transformHeader: (h) => h.trim()
                    });

                    // 5. Lưu vào MongoDB
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString,
                        totalRows: parsed.data.length,
                        parsedData: parsed.data
                    });
                    const savedDoc = await newFileEntry.save();

                    // 6. Phản hồi Lark Card thành công
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { 
                                    title: { tag: 'plain_text', content: '✅ Nhập Kho Thành Công' }, 
                                    template: "green" 
                                },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng:** ${parsed.data.length}\n🗄️ **ID:** \`${savedDoc._id}\`` } },
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
            console.error("❌ Lỗi xử lý:", error.message);
            // Gửi thông báo lỗi về cho người dùng Lark
            try {
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { 
                        msg_type: 'text', 
                        content: JSON.stringify({ text: `⚠️ Lỗi xử lý: ${error.message}` }) 
                    }
                });
            } catch (e) { console.error("Lỗi gửi tin nhắn báo lỗi:", e); }
        }
    }
    return res.status(200).json({ success: true });
});

// Webhook xử lý nút bấm trên Card
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa dữ liệu!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi khi xóa.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;