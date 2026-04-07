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
const USER_TOKEN = "t-g206478S3MT22QWRGGFSAIW6VXVOEE7HLGWN7N6A";

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
// 🟢 WEBHOOK 1: LOGIC MỚI - MỖI DÒNG CSV LÀ 1 TAB RIÊNG BIỆT
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

                    // --- BƯỚC 1: TẢI FILE CSV ---
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

                    // --- BƯỚC 2: TẠO SPREADSHEET TỔNG ---
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

                    // --- BƯỚC 3: ĐẶT TÊN TAB CHO TỪNG DÒNG (CHỐNG TRÙNG LẶP) ---
                    // Lark không cho phép 2 Tab trùng tên. Nên nếu có 3 dòng ngày 02.04.2026,
                    // Ta sẽ đặt tên là: 02.04.2026, 02.04.2026 (2), 02.04.2026 (3)
                    const nameCounter = {};
                    const tabRequests = [];
                    
                    rowsData.forEach(row => {
                        const dateKey = Object.keys(row).find(k => k.toLowerCase().includes('shipment date'));
                        let baseDate = dateKey && row[dateKey] ? String(row[dateKey]).trim() : 'Unknown_Date';
                        baseDate = baseDate.replace(/[\\/?*[\]:]/g, '-').substring(0, 25);

                        let finalTabName = baseDate;
                        if (!nameCounter[baseDate]) {
                            nameCounter[baseDate] = 1;
                        } else {
                            nameCounter[baseDate]++;
                            finalTabName = `${baseDate} (${nameCounter[baseDate]})`;
                        }
                        
                        row._tabName = finalTabName; // Lưu tên Tab vào row để lát nữa gọi đúng ID
                        tabRequests.push({ addSheet: { properties: { title: finalTabName } } });
                    });

                    // --- BƯỚC 4: TẠO HÀNG LOẠT CÁC TAB ---
                    console.log(`📑 Đang tạo ${tabRequests.length} Tab riêng biệt (Mỗi Waybill = 1 Tab)...`);
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    // --- BƯỚC 5: LẤY LẠI SHEET ID ---
                    console.log(`🔍 Truy vấn hệ thống để lấy Sheet ID...`);
                    const queryRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, {
                        method: 'GET',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}` }
                    });
                    const queryData = await queryRes.json();
                    const sheetIdMap = {};
                    if (queryData.data && queryData.data.sheets) {
                        queryData.data.sheets.forEach(sheet => { sheetIdMap[sheet.title] = sheet.sheet_id; });
                    }

                    // --- BƯỚC 6: LẶP QUA TỪNG DÒNG -> INSERT 1 DÒNG -> GHI WAYBILL VÀO B12 ---
                    console.log(`🚀 Bắt đầu Insert Dòng và ghi Waybill Number vào B12 cho từng Tab...`);
                    for (const r of rowsData) {
                        const targetSheetId = sheetIdMap[r._tabName];
                        if (!targetSheetId) continue;

                        const wbKey = Object.keys(r).find(k => k.toLowerCase().includes('waybill number'));
                        const wbValue = wbKey && r[wbKey] ? String(r[wbKey]) : "Không có dữ liệu";
                        const values = [[wbValue]];

                        // 🛠 6A: INSERT ĐÚNG 1 DÒNG Ở INDEX 11 (Vị trí dòng 12)
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/insert_dimension_range`, {
                            method: 'POST',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                dimension: { sheetId: targetSheetId, majorDimension: "ROWS", startIndex: 11, endIndex: 12 },
                                inheritStyle: "BEFORE"
                            })
                        });

                        // 🛠 6B: GHI 1 WAYBILL XUỐNG TỌA ĐỘ B12 CỦA TAB ĐÓ
                        const writeRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: `${targetSheetId}!B12:B12`, values: values } })
                        });
                        
                        const writeResult = await writeRes.json();
                        if (writeResult.code === 0) {
                            console.log(`   ✅ Đã ghi Waybill [${wbValue}] vào Tab [${r._tabName}]`);
                        } else {
                            console.error(`   ❌ Lỗi ghi Tab [${r._tabName}]: ${writeResult.msg}`);
                        }
                    }

                    // --- BƯỚC 7: LƯU DB & PHẢN HỒI ---
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
                                header: { title: { tag: 'plain_text', content: '✅ XỬ LÝ "MỖI DÒNG 1 TAB" HOÀN TẤT' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số Tab được tạo:** ${rowsData.length} Tab\n🚀 Đã tạo Tab độc lập cho từng Waybill và ghi thành công vào ô **B12**.` } },
                                    {
                                        tag: 'action',
                                        actions: [
                                            { tag: 'button', text: { tag: 'plain_text', content: '🌐 Mở Lark Sheet' }, type: 'primary', url: ssUrl },
                                            { tag: 'button', text: { tag: 'plain_text', content: '🗑️ Xóa DB' }, type: 'danger', value: { action: 'delete_file', docId: savedDoc._id } }
                                        ]
                                    }
                                ]
                            })
                        }
                    });
                    console.log(`🎉 HOÀN TẤT QUY TRÌNH!\n========================================`);
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