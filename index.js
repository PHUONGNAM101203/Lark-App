require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse'); 

const app = express();
app.use(express.json()); 

// ==========================================
// 1. KẾT NỐI MONGODB & TẠO BẢNG CHỨA DỮ LIỆU CSV
// ==========================================
let isConnected = false;
async function connectDB() {
    if (isConnected) return;
    try {
        await mongoose.connect(process.env.MONGODB_URI);
        isConnected = true;
        console.log('✅ Đã kết nối MongoDB (Chế độ Serverless)');
    } catch (err) {
        console.error('❌ Lỗi kết nối MongoDB:', err);
    }
}

// Bảng này sẽ nhận BẤT KỲ định dạng dữ liệu nào từ file CSV của bạn
const CsvDataSchema = new mongoose.Schema({
    rowData: mongoose.Schema.Types.Mixed, // Linh hoạt nhận mọi loại số lượng cột
    fileName: String,
    importedAt: { type: Date, default: Date.now }
});
// Dữ liệu sẽ được lưu vào collection tên là "csv_imports"
const CsvRecord = mongoose.model('CsvRecord', CsvDataSchema, 'csv_imports');

const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// ==========================================
// 2. BOT CHỈ LẮNG NGHE VÀ XỬ LÝ FILE
// ==========================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        // --- NẾU NGƯỜI DÙNG GỬI FILE ---
        if (message.message_type === 'file') {
            try {
                const fileContent = JSON.parse(message.content);
                const fileKey = fileContent.file_key;
                const fileName = fileContent.file_name;

                // Chỉ chấp nhận file .csv
                if (fileName.toLowerCase().endsWith('.csv')) {
                    
                    // 1. Báo đang xử lý
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: { content: JSON.stringify({ text: `⏳ Đang đọc và phân tích file: ${fileName}...` }), msg_type: 'text' }
                    });

                    // 2. Tải file về RAM
                    const fileData = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: fileKey },
                        params: { type: 'file' }
                    });

                    // 3. Chuyển Binary thành Text và parse bằng PapaParse
                    const csvString = Buffer.from(fileData).toString('utf8');
                    const parsedData = Papa.parse(csvString, {
                        header: true, // Lấy dòng đầu tiên làm tên cột
                        skipEmptyLines: true
                    });

                    const rows = parsedData.data;

                    // 4. Lưu vào MongoDB
                    if (rows.length > 0) {
                        await connectDB();

                        // Đóng gói từng dòng dữ liệu để đưa vào MongoDB
                        const recordsToSave = rows.map(row => ({
                            rowData: row,
                            fileName: fileName
                        }));

                        await CsvRecord.insertMany(recordsToSave);

                        // 5. Báo cáo thành công
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: `✅ Tuyệt vời! Đã nhập thành công ${rows.length} dòng dữ liệu từ file vào Database.` }), msg_type: 'text' }
                        });
                    } else {
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: { content: JSON.stringify({ text: `❌ File CSV trống, không có dữ liệu để lưu.` }), msg_type: 'text' }
                        });
                    }
                } else {
                    // Nhắc nhở nếu gửi sai đuôi file
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: { content: JSON.stringify({ text: `⚠️ Tôi chỉ có thể đọc được file định dạng .csv thôi nhé!` }), msg_type: 'text' }
                    });
                }
            } catch (error) {
                console.error("Lỗi xử lý file CSV:", error);
                await client.im.message.reply({
                    path: { message_id: message.message_id },
                    data: { content: JSON.stringify({ text: `❌ Đã xảy ra lỗi khi đọc file. File có thể bị lỗi font hoặc sai cấu trúc.` }), msg_type: 'text' }
                });
            }
        } 
        // --- NẾU NGƯỜI DÙNG CHAT CHỮ BÌNH THƯỜNG ---
        else if (message.message_type === 'text') {
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { content: JSON.stringify({ text: `👋 Chào bạn! Hãy ném một file dữ liệu (.csv) vào đây, tôi sẽ lập tức phân tích và lưu trữ giúp bạn.` }), msg_type: 'text' }
            });
        }
    }
    
    return res.status(200).json({ success: true });
});

// Xuất app cho Vercel
module.exports = app;