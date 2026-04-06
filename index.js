require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse'); 

const app = express();
app.use(express.json()); 

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

app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};
    
    // 🔴 RADAR: Ghi log toàn bộ gói tin Lark gửi đến vào Vercel
    console.log("📥 [LARK EVENT NHẬN ĐƯỢC]:", JSON.stringify(data, null, 2));

    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        try {
            // ----------------------------------------------------------------
            // 1. NẾU LÀ FILE VẬT LÝ
            // ----------------------------------------------------------------
            if (message.message_type === 'file') {
                const fileContent = JSON.parse(message.content);
                const fileName = fileContent.file_name;
                const fileKey = fileContent.file_key;

                // 🔴 BƯỚC XÁC NHẬN SỐ 1: Báo cáo ngay lập tức
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { content: JSON.stringify({ text: `📡 Radar xác nhận: Đã nhìn thấy file "${fileName}". Bắt đầu tải và phân tích...` }), msg_type: 'text' }
                });

                if (fileName.toLowerCase().endsWith('.csv')) {
                    const fileData = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: fileKey },
                        params: { type: 'file' }
                    });

                    const csvString = Buffer.from(fileData).toString('utf8');
                    const parsedData = Papa.parse(csvString, { header: true, skipEmptyLines: true });
                    const rows = parsedData.data;

                    if (rows.length > 0) {
                        await connectDB();
                        const recordsToSave = rows.map(row => ({ rowData: row, fileName: fileName }));
                        await CsvRecord.insertMany(recordsToSave);

                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: `✅ Tuyệt vời! Đã nhập thành công ${rows.length} dòng dữ liệu vào Database.` }), msg_type: 'text' }
                        });
                    } else {
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: `❌ File CSV rỗng!` }), msg_type: 'text' }
                        });
                    }
                } else {
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: { content: JSON.stringify({ text: `⚠️ Tôi chỉ xử lý được file .csv thôi nhé!` }), msg_type: 'text' }
                    });
                }
            } 
            // ----------------------------------------------------------------
            // 2. NẾU LÀ TIN NHẮN CHỮ HOẶC REPLY FILE TRONG NHÓM
            // ----------------------------------------------------------------
            else if (message.message_type === 'text') {
                let isReplyFile = false;
                
                // Kiểm tra xem có phải đang Reply một file không
                if (message.parent_id) {
                    const parentRes = await client.im.message.get({ path: { message_id: message.parent_id } });
                    if (parentRes.data.items[0].msg_type === 'file') {
                        isReplyFile = true;
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: `📡 Radar xác nhận: Bạn vừa Reply một file. Để code đơn giản, hiện tại hãy chat 1-1 và ném file thẳng cho tôi nhé!` }), msg_type: 'text' }
                        });
                    }
                }

                if (!isReplyFile) {
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: { content: JSON.stringify({ text: `👋 Chào bạn! Hãy thả trực tiếp một file .csv vào khung chat này để tôi xử lý.` }), msg_type: 'text' }
                    });
                }
            }
            // ----------------------------------------------------------------
            // 3. NẾU LÀ CÁC LOẠI KHÁC (ẢNH, VIDEO, TÀI LIỆU LARK DOCS)
            // ----------------------------------------------------------------
            else {
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { content: JSON.stringify({ text: `⚠️ Radar phát hiện: Bạn vừa gửi định dạng "${message.message_type}". Đây không phải là file vật lý, hãy tải xuống thành .csv và gửi lại nhé.` }), msg_type: 'text' }
                });
            }
        } catch (error) {
            console.error("❌ LỖI NGHIÊM TRỌNG:", error);
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { content: JSON.stringify({ text: `❌ Code bị sập giữa chừng! Hãy xem Vercel Logs.` }), msg_type: 'text' }
            });
        }
    }
    
    return res.status(200).json({ success: true });
});

module.exports = app;