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

// ==========================================
// 2. XỬ LÝ NHẬN VÀ LẶP FILE CSV
// ==========================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        try {
            if (message.message_type === 'file') {
                const fileContent = JSON.parse(message.content);
                const fileName = fileContent.file_name;
                const fileKey = fileContent.file_key;

                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { content: JSON.stringify({ text: `⏳ Đang tải và bóc tách dữ liệu từ file "${fileName}"...` }), msg_type: 'text' }
                });

                if (fileName.toLowerCase().endsWith('.csv')) {
                    // Tải file từ Lark về bằng fileKey
                    const fileData = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: fileKey },
                        params: { type: 'file' }
                    });

                    // Parse dữ liệu từ file
                    const csvString = fileData.toString('utf8');
                    const parsedData = Papa.parse(csvString, { header: true, skipEmptyLines: true });
                    const rows = parsedData.data;

                    if (rows.length > 0) {
                        await connectDB();
                        let previewText = "";
                        let successCount = 0;

                        // 🔄 VÒNG LẶP QUA TỪNG ROW CHÍNH XÁC NHƯ YÊU CẦU CỦA BẠN
                        for (let i = 0; i < rows.length; i++) {
                            const row = rows[i]; // Lấy dữ liệu của 1 dòng
                            
                            // Lưu dòng này vào MongoDB
                            await CsvRecord.create({ rowData: row, fileName: fileName });
                            successCount++;

                            // Lấy 3 dòng đầu tiên để in ra cho bạn xem chứng minh bot đã đọc được
                            if (i < 3) {
                                // Lấy tất cả các cột của dòng đó gom thành 1 đoạn text
                                const rowValues = Object.entries(row).map(([key, value]) => `[${key}: ${value}]`).join(' | ');
                                previewText += `\n👉 Dòng ${i + 1}: ${rowValues}`;
                            }
                        }

                        // Xây dựng câu trả lời báo cáo kết quả
                        let replyMessage = `✅ Đã lặp và lưu thành công ${successCount} dòng!\n`;
                        replyMessage += `👀 Dưới đây là dữ liệu 3 dòng đầu tiên tôi lấy được:${previewText}`;
                        if (rows.length > 3) {
                            replyMessage += `\n... và ${rows.length - 3} dòng nữa đã được lưu an toàn.`;
                        }

                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: replyMessage }), msg_type: 'text' }
                        });
                    } else {
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: `❌ File CSV rỗng, không có dòng nào!` }), msg_type: 'text' }
                        });
                    }
                } else {
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: { content: JSON.stringify({ text: `⚠️ Tôi chỉ xử lý được file .csv thôi nhé!` }), msg_type: 'text' }
                    });
                }
            } 
            else if (message.message_type === 'text') {
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { content: JSON.stringify({ text: `👋 Chào bạn! Hãy ném một file .csv có nhiều dòng vào đây, tôi sẽ loop qua từng dòng cho bạn xem.` }), msg_type: 'text' }
                });
            }
        } catch (error) {
            console.error("❌ LỖI KHI XỬ LÝ:", error);
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { content: JSON.stringify({ text: `❌ Lỗi rồi! Không thể tải hoặc đọc file. Hãy kiểm tra Vercel Logs.` }), msg_type: 'text' }
            });
        }
    }
    
    return res.status(200).json({ success: true });
});

module.exports = app;