require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');

const app = express();
app.use(express.json());

// ==========================================
// 1. KẾT NỐI MONGODB (Serverless Pattern)
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

// Định nghĩa Schema lưu trữ File và Data
const CsvVaultSchema = new mongoose.Schema({
    fileName: String,
    fileContentRaw: String,    // Nội dung nguyên bản của file CSV (Text)
    totalRows: Number,         // Tổng số dòng dữ liệu
    parsedData: [mongoose.Schema.Types.Mixed], // Mảng chứa các object dòng dữ liệu
    importedAt: { type: Date, default: Date.now }
});

const CsvVault = mongoose.models.CsvVault || mongoose.model('CsvVault', CsvVaultSchema, 'csv_storage');

// Khởi tạo Lark Client
const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// =====================================================================
// 2. WEBHOOK CHÍNH: NHẬN SỰ KIỆN (Event Callback)
// Link: https://your-app.vercel.app/webhook/event
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    // Xác thực URL với Lark (Challenge)
    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    // Xử lý sự kiện nhận tin nhắn
    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        try {
            // Chỉ xử lý nếu tin nhắn là dạng FILE
            if (message.message_type === 'file') {
                const { file_name, file_key } = JSON.parse(message.content);

                // Kiểm tra nếu là file CSV
                if (file_name.toLowerCase().endsWith('.csv')) {
                    console.log(`📂 Đang xử lý file CSV: ${file_name}`);

                    // BƯỚC 1: Tải file từ Lark (trả về Buffer)
                    const fileBuffer = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: file_key },
                        params: { type: 'file' }
                    });

                    // BƯỚC 2: Chuyển Buffer thành String (Đọc file trong RAM)
                    const csvString = fileBuffer.toString('utf-8');

                    // BƯỚC 3: Parse CSV sang JSON bằng PapaParse
                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: true,
                        dynamicTyping: true
                    });

                    // BƯỚC 4: Lưu vào MongoDB
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString, // Lưu file thô vào DB
                        totalRows: parsed.data.length,
                        parsedData: parsed.data     // Lưu dữ liệu đã bóc tách
                    });
                    const savedDoc = await newFileEntry.save();

                    // BƯỚC 5: Trả lời Card cho người dùng
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
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng:** ${parsed.data.length}\n🗄️ **Database:** Đã lưu vào bộ sưu tập \`csv_storage\`` } },
                                    {
                                        tag: 'action',
                                        actions: [{
                                            tag: 'button',
                                            text: { tag: 'plain_text', content: '🗑️ Xóa bản ghi này' },
                                            type: 'danger',
                                            value: { action: 'delete_file', docId: savedDoc._id }
                                        }]
                                    }
                                ]
                            })
                        }
                    });
                }
            } else {
                // Nếu là tin nhắn văn bản bình thường
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { 
                        msg_type: 'text', 
                        content: JSON.stringify({ text: "Chào bạn! Hãy gửi cho tôi 1 file .csv để tôi lưu vào Database nhé." }) 
                    }
                });
            }
        } catch (error) {
            console.error("❌ Lỗi xử lý Webhook:", error);
        }
    }
    return res.status(200).json({ success: true });
});

// =====================================================================
// 3. WEBHOOK PHỤ: XỬ LÝ NÚT BẤM TRÊN CARD (Card Callback)
// Link: https://your-app.vercel.app/webhook/card
// =====================================================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};

    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            
            return res.status(200).json({
                toast: { type: 'success', content: 'Đã xóa bản ghi khỏi MongoDB!' }
            });
        } catch (err) {
            return res.status(200).json({
                toast: { type: 'error', content: 'Lỗi khi xóa dữ liệu.' }
            });
        }
    }
    return res.status(200).json({ success: true });
});

// Export cho Vercel
module.exports = app;

// Nếu chạy local (Node.js bình thường)
if (process.env.NODE_ENV !== 'production') {
    const PORT = 3000;
    app.listen(PORT, () => console.log(`🚀 Server running on http://localhost:${PORT}`));
}