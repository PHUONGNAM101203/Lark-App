require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');

const app = express();
app.use(express.json());

// ==========================================
// 🔑 TOKEN USER (Đảm bảo Token còn hạn)
// ==========================================
const USER_TOKEN = "t-g206477nRTN6WHRWMKRNWVCQ457DCZM5LLEWVBAQ";

// ==========================================
// 🛡 BỘ LỌC CHỐNG LARK SPAM
// ==========================================
const processedEvents = new Set();

// ==========================================
// 🗄 KẾT NỐI MONGODB
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
        console.log('✅ Đã kết nối MongoDB thành công');
    } catch (err) {
        console.error('❌ Lỗi kết nối MongoDB:', err);
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

function getColLetter(index) {
    let letter = '';
    while (index >= 0) {
        letter = String.fromCharCode((index % 26) + 65) + letter;
        index = Math.floor(index / 26) - 1;
    }
    return letter;
}

// =====================================================================
// 🟢 WEBHOOK 1: NHẬN FILE & TẠO SPREADSHEET CÓ MỞ RỘNG KÍCH THƯỚC
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
                    console.log(`\n========================================`);
                    console.log(`📂 BẮT ĐẦU XỬ LÝ FILE: ${file_name}`);
                    
                    // --- 1. TẢI FILE & ĐỌC DỮ LIỆU ---
                    const tokenRes = await client.auth.tenantAccessToken.internal({
                        data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET }
                    });
                    const tenantToken = tokenRes.tenant_access_token;
                    
                    const fileUrl = `https://open.larksuite.com/open-apis/im/v1/messages/${message.message_id}/resources/${file_key}?type=file`;
                    const fetchRes = await fetch(fileUrl, { headers: { 'Authorization': `Bearer ${tenantToken}` } });
                    if (!fetchRes.ok) throw new Error(`Lỗi HTTP: ${fetchRes.statusText}`);

                    const fileBuffer = Buffer.from(await fetchRes.arrayBuffer());
                    let csvString = fileBuffer.toString('utf-8').replace(/^\uFEFF/, '');
                    if (!csvString.trim()) throw new Error("File rỗng.");

                    const parsed = Papa.parse(csvString, {
                        header: true, skipEmptyLines: 'greedy', dynamicTyping: true, transformHeader: (h) => h.trim()
                    });
                    const rowsData = parsed.data;

                    // --- 2. GỘP NHÓM THEO SHIPMENT DATE ---
                    const groupedByDate = {};
                    rowsData.forEach(row => {
                        const dateKey = Object.keys(row).find(k => k.toLowerCase().includes('shipment date'));
                        let dateVal = dateKey && row[dateKey] ? String(row[dateKey]).trim() : 'Unknown_Date';
                        dateVal = dateVal.replace(/[\\/?*[\]:]/g, '-').substring(0, 31);
                        
                        if (!groupedByDate[dateVal]) groupedByDate[dateVal] = [];
                        groupedByDate[dateVal].push(row);
                    });

                    // --- 3. TẠO SPREADSHEET MỚI ---
                    console.log(`📝 Đang tạo Lark Spreadsheet...`);
                    const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ title: `Báo Cáo: ${file_name}` })
                    });
                    const createData = await createRes.json();
                    if (createData.code !== 0) throw new Error(`Lỗi tạo Spreadsheet: ${createData.msg}`);
                    
                    const ssToken = createData.data.spreadsheet.spreadsheet_token;
                    const ssUrl = createData.data.spreadsheet.url;

                    // --- 4. TẠO TẤT CẢ CÁC TAB VÀ LẤY SHEET_ID ---
                    const sheetNames = Object.keys(groupedByDate);
                    console.log(`📑 Đang tạo ${sheetNames.length} Tab theo ngày...`);
                    const tabRequests = sheetNames.map(name => ({ addSheet: { properties: { title: name } } }));
                    
                    const batchRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });
                    const batchData = await batchRes.json();

                    // Bóc tách Sheet_ID của từng Tab vừa tạo (để phục vụ việc thêm Dòng/Cột)
                    const sheetIdMap = {};
                    if (batchData.data && batchData.data.replies) {
                        batchData.data.replies.forEach(reply => {
                            if (reply.addSheet && reply.addSheet.properties) {
                                sheetIdMap[reply.addSheet.properties.title] = reply.addSheet.properties.sheetId;
                            }
                        });
                    }

                    // --- 5. BƠM DỮ LIỆU (CÓ CƠ CHẾ TỰ ĐỘNG THÊM DÒNG/CỘT) ---
                    console.log(`🚀 Bắt đầu đo đạc và bơm dữ liệu...`);
                    for (const sheetName of sheetNames) {
                        const rows = groupedByDate[sheetName];
                        if (rows.length === 0) continue;
                        
                        const headers = Object.keys(rows[0]);
                        const values = [headers];
                        rows.forEach(r => values.push(headers.map(h => r[h] !== null && r[h] !== undefined ? String(r[h]) : "")));

                        const targetSheetId = sheetIdMap[sheetName];

                        // 🛠 BƯỚC 5A: THÊM DÒNG (Nếu dữ liệu > 200 dòng mặc định)
                        if (targetSheetId && values.length > 200) {
                            let rowsToAdd = values.length - 200 + 5; // Cộng dư 5 dòng cho thoải mái
                            
                            // API chỉ cho thêm max 4999 dòng 1 lần, dùng vòng lặp để add nếu data cực lớn
                            while (rowsToAdd > 0) {
                                const addLength = Math.min(rowsToAdd, 4999);
                                await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/dimension_range`, {
                                    method: 'POST',
                                    headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        dimension: { sheetId: targetSheetId, majorDimension: "ROWS", length: addLength }
                                    })
                                });
                                rowsToAdd -= addLength;
                            }
                            console.log(`   ➕ Đã thêm dòng cho Sheet [${sheetName}]`);
                        }

                        // 🛠 BƯỚC 5B: THÊM CỘT (Nếu dữ liệu > 20 cột mặc định)
                        if (targetSheetId && headers.length > 20) {
                            let colsToAdd = headers.length - 20 + 2;
                            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/dimension_range`, {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    dimension: { sheetId: targetSheetId, majorDimension: "COLUMNS", length: colsToAdd }
                                })
                            });
                            console.log(`   ➕ Đã thêm cột cho Sheet [${sheetName}]`);
                        }

                        // 🛠 BƯỚC 5C: BƠM DỮ LIỆU
                        const endColLetter = getColLetter(headers.length - 1);
                        const range = `${sheetName}!A1:${endColLetter}${values.length}`;

                        const writeRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: range, values: values } })
                        });
                        
                        const writeResult = await writeRes.json();
                        if (writeResult.code === 0) {
                            console.log(`   ✅ Ghi thành công data vào [${sheetName}]`);
                        } else {
                            console.error(`   ❌ Lỗi ghi [${sheetName}]: ${writeResult.msg}`);
                        }
                    }

                    // --- 6. LƯU MONGODB & TRẢ LỜI ---
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData
                    });
                    const savedDoc = await newFileEntry.save();

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ TẠO SHEET HOÀN TẤT' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số Shipment Date:** ${sheetNames.length}\n🗂️ **Tổng dòng dữ liệu:** ${rowsData.length}\n*(Đã tự động cơi nới mở rộng Bảng tính)*` } },
                                    {
                                        tag: 'action',
                                        actions: [
                                            { tag: 'button', text: { tag: 'plain_text', content: '🌐 Mở Lark Sheet' }, type: 'primary', url: ssUrl },
                                            { tag: 'button', text: { tag: 'plain_text', content: '🗑️ Xóa Database' }, type: 'danger', value: { action: 'delete_file', docId: savedDoc._id } }
                                        ]
                                    }
                                ]
                            })
                        }
                    });
                    console.log(`🎉 HOÀN THÀNH QUY TRÌNH!\n========================================`);
                }
            }
        } catch (error) {
            console.error("\n❌ LỖI NGHIÊM TRỌNG:", error.message);
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi hệ thống: ${error.message}` }) }
            });
        }
    }
    return res.status(200).json({ success: true });
});

// =====================================================================
// 🔵 WEBHOOK 2: XỬ LÝ NÚT BẤM
// =====================================================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    if (data.type === 'url_verification') return res.status(200).json({ challenge: data.challenge });

    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa dữ liệu gốc!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi khi xóa.' } });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;
if (process.env.NODE_ENV !== 'production') {
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
        console.log(`🚀 Server đang chạy tại Port: ${PORT}`);
    });
}