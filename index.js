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
const USER_TOKEN = "u-dSzpHKprtcaEjR6XpgCR3Pl04Tsqh5UXNgGavxs020k2";

const processedEvents = new Set();

// ==========================================
// 🗄 KẾT NỐI MONGODB
// ==========================================
let isConnected = false;
async function connectDB() {
    if (isConnected) return;
    try {
        if (mongoose.connections[0].readyState) { isConnected = true; return; }
        await mongoose.connect(process.env.MONGODB_URI);
        isConnected = true;
        console.log('✅ Đã kết nối MongoDB');
    } catch (err) { console.error('❌ Lỗi kết nối MongoDB:', err); }
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
// 🟢 WEBHOOK 1: TẠO SHEET -> LẤY ID -> INSERT DÒNG -> GHI VÀO B12
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') return res.status(200).json({ challenge: data.challenge });

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

                    // --- BƯỚC 1: TẢI FILE ---
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

                    // --- BƯỚC 2: GỘP NHÓM THEO "SHIPMENT DATE" ---
                    const groupedByDate = {};
                    rowsData.forEach(row => {
                        const dateKey = Object.keys(row).find(k => k.toLowerCase().includes('shipment date'));
                        let dateVal = dateKey && row[dateKey] ? String(row[dateKey]).trim() : 'Unknown_Date';
                        dateVal = dateVal.replace(/[\\/?*[\]:]/g, '-').substring(0, 31);
                        
                        if (!groupedByDate[dateVal]) groupedByDate[dateVal] = [];
                        groupedByDate[dateVal].push(row);
                    });

                    // --- BƯỚC 3: TẠO SPREADSHEET VÀ TAB MỚI ---
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

                    const sheetNames = Object.keys(groupedByDate);
                    console.log(`📑 Đang tạo ${sheetNames.length} Tab...`);
                    const tabRequests = sheetNames.map(name => ({ addSheet: { properties: { title: name } } }));
                    
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    // --- BƯỚC 4: GET LẠI TOÀN BỘ SHEET ID ---
                    console.log(`🔍 Truy vấn hệ thống để lấy Sheet ID chuẩn xác...`);
                    const queryRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, {
                        method: 'GET',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}` }
                    });
                    const queryData = await queryRes.json();
                    if (queryData.code !== 0) throw new Error(`Lỗi truy vấn Sheet ID: ${queryData.msg}`);

                    const sheetIdMap = {};
                    if (queryData.data && queryData.data.sheets) {
                        queryData.data.sheets.forEach(sheet => { sheetIdMap[sheet.title] = sheet.sheet_id; });
                    }

                    // --- BƯỚC 5: INSERT DÒNG & BƠM DATA VÀO B12 ---
                    console.log(`🚀 Bắt đầu TEST hàm Insert Rows và ghi Recipient Name vào B12...`);
                    for (const sheetName of sheetNames) {
                        const rows = groupedByDate[sheetName];
                        if (rows.length === 0) continue;
                        
                        const targetSheetId = sheetIdMap[sheetName];
                        if (!targetSheetId) continue;

                        // Tìm cột "Recipient Name"
                        const headers = Object.keys(rows[0]);
                        const targetKey = headers.find(h => h.toLowerCase().includes('recipient name'));

                        // BỎ TIÊU ĐỀ: Chỉ bốc duy nhất giá trị (List)
                        const values = [];
                        rows.forEach(r => {
                            values.push([ targetKey && r[targetKey] ? String(r[targetKey]) : "Không có dữ liệu" ]);
                        });

                        if (values.length === 0) continue;

                        // 🛠 TÍNH TOÁN INDEX ĐỂ INSERT (Dòng 12 = Index 11)
                        const insertStartIndex = 11; 
                        const insertEndIndex = insertStartIndex + values.length; 

                        console.log(`   ➕ Đang Insert ${values.length} dòng vào vị trí từ Index ${insertStartIndex} đến ${insertEndIndex}`);
                        
                        const insertRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/insert_dimension_range`, {
                            method: 'POST',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                dimension: {
                                    sheetId: targetSheetId,
                                    majorDimension: "ROWS",
                                    startIndex: insertStartIndex,
                                    endIndex: insertEndIndex
                                },
                                inheritStyle: "BEFORE"
                            })
                        });
                        const insertData = await insertRes.json();
                        if (insertData.code !== 0) {
                            console.error(`   ❌ Lỗi Insert Dòng: ${insertData.msg}`);
                        }

                        // 🛠 GHI DỮ LIỆU XUỐNG TỌA ĐỘ B12
                        // Độ dài range = B12 đến B(12 + số lượng list - 1)
                        const endRow = 12 + values.length - 1; 
                        const range = `${targetSheetId}!B12:B${endRow}`;

                        const writeRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: range, values: values } })
                        });
                        
                        const writeResult = await writeRes.json();
                        if (writeResult.code === 0) {
                            console.log(`   ✅ Đã ghi danh sách Recipient Name vào tọa độ ${range} [${sheetName}]`);
                        } else {
                            console.error(`   ❌ Lỗi ghi dữ liệu [${sheetName}]: ${writeResult.msg}`);
                        }
                    }

                    // --- BƯỚC 6: LƯU DB & PHẢN HỒI ---
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
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n🚀 Đã chèn (Insert) dòng từ **Index 11**.\n🎯 Đã ghi danh sách \`Recipient Name\` vào tọa độ **B12** (Không có tiêu đề).` } },
                                    {
                                        tag: 'action',
                                        actions: [
                                            { tag: 'button', text: { tag: 'plain_text', content: '🌐 Kiểm tra kết quả' }, type: 'primary', url: ssUrl },
                                            { tag: 'button', text: { tag: 'plain_text', content: '🗑️ Xóa DB' }, type: 'danger', value: { action: 'delete_file', docId: savedDoc._id } }
                                        ]
                                    }
                                ]
                            })
                        }
                    });
                    console.log(`🎉 HOÀN TẤT BÀI TEST!\n========================================`);
                }
            }
        } catch (error) {
            console.error("\n❌ LỖI HỆ THỐNG:", error.message);
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
    if (data.type === 'url_verification') return res.status(200).json({ challenge: data.challenge });

    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa dữ liệu gốc!' } });
        } catch (err) { return res.status(200).json({ toast: { type: 'error', content: 'Lỗi khi xóa.' } }); }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;
if (process.env.NODE_ENV !== 'production') app.listen(process.env.PORT || 3000, () => console.log(`🚀 Server running!`));