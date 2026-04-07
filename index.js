require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse'); 

const app = express();
app.use(express.json()); 

// ==========================================
// KẾT NỐI MONGODB
// ==========================================
let isConnected = false;
async function connectDB() {
    if (isConnected) return;
    try {
        await mongoose.connect(process.env.MONGODB_URI);
        isConnected = true;
        console.log('✅ Đã kết nối MongoDB');
    } catch (err) {
        console.error('❌ Lỗi kết nối MongoDB:', err);
    }
}

const CsvDataSchema = new mongoose.Schema({
    rowData: mongoose.Schema.Types.Mixed,
    fileName: String,
    importedAt: { type: Date, default: Date.now }
});
const CsvRecord = mongoose.model('CsvRecord', CsvDataSchema, 'csv_imports');

const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});


// =====================================================================
// 🟢 WEBHOOK 1: CHUYÊN XỬ LÝ SỰ KIỆN (Nhận tin nhắn, Nhận File)
// Link sử dụng: https://[ten-app-cua-ban].vercel.app/webhook/event
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};
    console.log("📥 [WEBHOOK 1 - EVENT] Nhận tín hiệu:", JSON.stringify(data.header || data, null, 2));

    // Vượt qua vòng kiểm tra của Lark
    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    // Xử lý khi có người gửi file CSV
    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;
        try {
            if (message.message_type === 'file') {
                const fileContent = JSON.parse(message.content);
                const fileName = fileContent.file_name;
                const fileKey = fileContent.file_key;

                if (fileName.toLowerCase().endsWith('.csv')) {
                    const fileData = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: fileKey },
                        params: { type: 'file' }
                    });

                    const csvString = fileData.toString('utf8');
                    const parsedData = Papa.parse(csvString, { header: true, skipEmptyLines: true });
                    const rows = parsedData.data;

                    if (rows.length > 0) {
                        await connectDB();
                        const recordsToSave = rows.map(row => ({ rowData: row, fileName: fileName }));
                        await CsvRecord.insertMany(recordsToSave);

                        // Gửi thẻ Card để Test Webhook 2
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: {
                                msg_type: 'interactive',
                                content: JSON.stringify({
                                    header: { title: { tag: 'plain_text', content: '✅ Import Thành Công!' }, template: "green" },
                                    elements: [
                                        { tag: 'div', text: { tag: 'lark_md', content: `Đã lưu thành công **${rows.length}** dòng dữ liệu từ file ${fileName}.` } },
                                        {
                                            tag: 'action',
                                            actions: [{
                                                tag: 'button',
                                                text: { tag: 'plain_text', content: '🗑️ Xóa dữ liệu vừa nhập' },
                                                type: 'danger',
                                                value: { action: 'delete_recent', targetFile: fileName }
                                            }]
                                        }
                                    ]
                                })
                            }
                        });
                    }
                }
            } 
            else if (message.message_type === 'text') {
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { content: JSON.stringify({ text: `Hệ thống chia luồng đã sẵn sàng! Hãy ném file CSV vào đây.` }), msg_type: 'text' }
                });
            }
        } catch (error) {
            console.error("❌ LỖI WEBHOOK 1:", error);
        }
    }
    return res.status(200).json({ success: true });
});


// =====================================================================
// 🔵 WEBHOOK 2: CHUYÊN XỬ LÝ NÚT BẤM (Card Callback)
// Link sử dụng: https://[ten-app-cua-ban].vercel.app/webhook/card
// =====================================================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    console.log("👆 [WEBHOOK 2 - CARD CALLBACK] Có lượt bấm nút:", JSON.stringify(data, null, 2));

    // Vượt qua vòng kiểm tra của Lark cho Card
    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    // Khi người dùng bấm nút "Xóa dữ liệu vừa nhập"
    if (data.action && data.action.value && data.action.value.action === 'delete_recent') {
        await connectDB();
        const fName = data.action.value.targetFile;
        
        // Xóa các record có tên file tương ứng
        await CsvRecord.deleteMany({ fileName: fName });
        console.log(`Đã xóa dữ liệu của file: ${fName}`);

        // Trả về thông báo Popup (Toast) ngay trên màn hình Lark
        return res.status(200).json({
            toast: {
                type: 'info',
                content: `Đã dọn dẹp sạch sẽ dữ liệu của ${fName} khỏi Database!`
            }
        });
    }

    return res.status(200).json({ success: true });
});

module.exports = app;