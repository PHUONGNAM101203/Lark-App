require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');

const app = express();
app.use(express.json());

// ==========================================
// 1. BỘ LỌC CHỐNG TRÙNG LẶP
// ==========================================
const processedEvents = new Set();

// ==========================================
// 2. KẾT NỐI MONGODB
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

// =====================================================================
// 3. WEBHOOK CHÍNH
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    const eventId = data.header && data.header.event_id;
    if (eventId) {
        if (processedEvents.has(eventId)) return res.status(200).json({ success: true });
        processedEvents.add(eventId);
        setTimeout(() => processedEvents.delete(eventId), 10 * 60 * 1000); 
    }

    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        try {
            if (message.message_type === 'file') {
                const { file_name, file_key } = JSON.parse(message.content);

                if (file_name.toLowerCase().endsWith('.csv')) {
                    console.log(`\n📂 Bắt đầu tải file: ${file_name}`);

                    const tokenRes = await client.auth.tenantAccessToken.internal({
                        data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET }
                    });
                    const token = tokenRes.tenant_access_token;

                    const fileUrl = `https://open.larksuite.com/open-apis/im/v1/messages/${message.message_id}/resources/${file_key}?type=file`;
                    const fetchRes = await fetch(fileUrl, {
                        method: 'GET',
                        headers: { 'Authorization': `Bearer ${token}` }
                    });

                    if (!fetchRes.ok) throw new Error(`Lỗi tải file: ${fetchRes.statusText}`);

                    const arrayBuffer = await fetchRes.arrayBuffer();
                    const fileBuffer = Buffer.from(arrayBuffer);
                    let csvString = fileBuffer.toString('utf-8');
                    csvString = csvString.replace(/^\uFEFF/, ''); 

                    if (!csvString.trim()) throw new Error("Nội dung file rỗng.");

                    const parsed = Papa.parse(csvString, {
                        header: true,
                        skipEmptyLines: 'greedy',
                        dynamicTyping: true,
                        transformHeader: (h) => h.trim() 
                    });

                    const rowsData = parsed.data;

                    // ========================================================
                    // ✅ UPDATE: ĐỌC ĐỘNG MỌI CỘT TRONG FILE
                    // ========================================================
                    console.log(`\n--- KẾT QUẢ ĐỌC FILE: ${file_name} ---`);
                    rowsData.forEach((row, index) => { 
                        console.log(`[Dòng ${index + 1}]:`); 
                        
                        // Object.entries sẽ tự động lấy tất cả "Tên Cột" và "Giá Trị" của dòng đó
                        for (const [columnName, value] of Object.entries(row)) {
                            console.log(`   🔸 ${columnName}: ${value}`);
                        }
                    });
                    console.log("------------------------------------\n");
                    // ========================================================

                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name,
                        fileContentRaw: csvString,
                        totalRows: rowsData.length,
                        parsedData: rowsData
                    });
                    const savedDoc = await newFileEntry.save();

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ Đã Nhập DB Thành Công' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số dòng:** ${rowsData.length}\n🗄️ **ID Bản ghi:** \`${savedDoc._id}\`` } },
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
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi xử lý: ${error.message}` }) }
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
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa dữ liệu khỏi Database!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi xóa bản ghi.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;
if (process.env.NODE_ENV !== 'production') {
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => console.log(`🚀 Server đang chạy trên port ${PORT}`));
}