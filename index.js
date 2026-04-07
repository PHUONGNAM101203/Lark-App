require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse'); 
const fs = require('fs'); // Thêm để xử lý file
const path = require('path'); // Thêm để xử lý đường dẫn

const app = express();
app.use(express.json()); 

// 📁 Cấu hình thư mục lưu trữ file
const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) {
    fs.mkdirSync(UPLOAD_DIR);
    console.log('📁 Đã tạo thư mục uploads');
}

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
    localPath: String, // Lưu thêm đường dẫn file cục bộ
    importedAt: { type: Date, default: Date.now }
});
const CsvRecord = mongoose.model('CsvRecord', CsvDataSchema, 'csv_imports');

const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// =====================================================================
// 🟢 WEBHOOK 1: XỬ LÝ NHẬN FILE & LƯU TRỮ CỤC BỘ
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
                const fileContentInfo = JSON.parse(message.content);
                const fileName = fileContentInfo.file_name;
                const fileKey = fileContentInfo.file_key;

                if (fileName.toLowerCase().endsWith('.csv')) {
                    console.log(`🚀 Bắt đầu xử lý file: ${fileName}`);

                    // 1. Tải file từ server Lark về (trả về Buffer)
                    const fileBuffer = await client.im.messageResource.get({
                        path: { message_id: message.message_id, file_key: fileKey },
                        params: { type: 'file' }
                    });

                    // 2. Lưu file vật lý vào thư mục /uploads
                    // Thêm timestamp vào tên file để tránh trùng lặp
                    const safeFileName = `${Date.now()}_${fileName}`;
                    const filePath = path.join(UPLOAD_DIR, safeFileName);
                    
                    fs.writeFileSync(filePath, fileBuffer);
                    console.log(`💾 Đã lưu file tại: ${filePath}`);

                    // 3. Dùng PapaParse đọc file từ thư mục vừa lưu
                    const csvContent = fs.readFileSync(filePath, 'utf8');
                    const parsedData = Papa.parse(csvContent, { 
                        header: true, 
                        skipEmptyLines: true,
                        dynamicTyping: true // Tự động chuyển số/ngày tháng đúng kiểu
                    });

                    const rows = parsedData.data;

                    if (rows.length > 0) {
                        await connectDB();
                        const recordsToSave = rows.map(row => ({ 
                            rowData: row, 
                            fileName: fileName,
                            localPath: filePath 
                        }));
                        await CsvRecord.insertMany(recordsToSave);

                        // 4. Phản hồi cho người dùng qua Lark Card
                        await client.im.message.reply({
                            path: { message_id: message.message_id },
                            data: {
                                msg_type: 'interactive',
                                content: JSON.stringify({
                                    header: { title: { tag: 'plain_text', content: '📂 Đã Lưu File & Data' }, template: "blue" },
                                    elements: [
                                        { tag: 'div', text: { tag: 'lark_md', content: `📝 **Tên file:** ${fileName}\n📍 **Vị trí lưu:** \`/uploads/${safeFileName}\` \n📊 **Số dòng:** ${rows.length}` } },
                                        {
                                            tag: 'action',
                                            actions: [{
                                                tag: 'button',
                                                text: { tag: 'plain_text', content: '🗑️ Xóa dữ liệu' },
                                                type: 'danger',
                                                value: { action: 'delete_recent', targetFile: fileName, pathToDelete: filePath }
                                            }]
                                        }
                                    ]
                                })
                            }
                        });
                    }
                }
            }
        } catch (error) {
            console.error("❌ LỖI XỬ LÝ FILE:", error);
        }
    }
    return res.status(200).json({ success: true });
});

// =====================================================================
// 🔵 WEBHOOK 2: XỬ LÝ NÚT BẤM (Xóa cả DB và File vật lý)
// =====================================================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};

    if (data.action && data.action.value && data.action.value.action === 'delete_recent') {
        const { targetFile, pathToDelete } = data.action.value;
        
        try {
            await connectDB();
            // Xóa trong Database
            await CsvRecord.deleteMany({ fileName: targetFile });

            // Xóa file vật lý trong thư mục uploads
            if (fs.existsSync(pathToDelete)) {
                fs.unlinkSync(pathToDelete);
                console.log(`🗑️ Đã xóa file vật lý: ${pathToDelete}`);
            }

            return res.status(200).json({
                toast: { type: 'success', content: `Đã xóa sạch dữ liệu và file ${targetFile}!` }
            });
        } catch (err) {
            return res.status(200).json({
                toast: { type: 'error', content: `Lỗi khi xóa: ${err.message}` }
            });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;