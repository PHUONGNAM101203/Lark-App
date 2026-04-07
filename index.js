require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');

const app = express();
app.use(express.json());

// ==========================================
// THẺ BÀI USER (Token bạn cung cấp)
// ==========================================
const USER_TOKEN = "u-feFCdznf94C9pR1EUToqfCg06iHqh5oNrw0GeAk02ETM";

// ==========================================
// 1. BỘ LỌC CHỐNG TRÙNG LẶP SỰ KIỆN
// ==========================================
const processedEvents = new Set();

// ==========================================
// 2. KẾT NỐI MONGODB
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

// ✅ HÀM BỔ TRỢ: Tính chữ cái của cột Excel (Ví dụ: 0 -> A, 1 -> B, 26 -> AA)
function getColLetter(index) {
    let letter = '';
    while (index >= 0) {
        letter = String.fromCharCode((index % 26) + 65) + letter;
        index = Math.floor(index / 26) - 1;
    }
    return letter;
}

// =====================================================================
// 3. WEBHOOK CHÍNH
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') return res.status(200).json({ challenge: data.challenge });

    // Lọc sự kiện trùng lặp
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
                    console.log(`\n📂 Đang xử lý: ${file_name}`);

                    // --- BƯỚC 1: LẤY TOKEN VÀ TẢI FILE CSV ---
                    const tokenRes = await client.auth.tenantAccessToken.internal({
                        data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET }
                    });
                    const tenantToken = tokenRes.tenant_access_token;
                    
                    // Ưu tiên dùng User Token của bạn, nếu lỗi/hết hạn thì tự động lùi về Tenant Token của Bot
                    const activeToken = USER_TOKEN || tenantToken;

                    const fileUrl = `https://open.larksuite.com/open-apis/im/v1/messages/${message.message_id}/resources/${file_key}?type=file`;
                    const fetchRes = await fetch(fileUrl, { headers: { 'Authorization': `Bearer ${tenantToken}` } });
                    if (!fetchRes.ok) throw new Error("Lỗi tải file gốc");

                    const fileBuffer = Buffer.from(await fetchRes.arrayBuffer());
                    let csvString = fileBuffer.toString('utf-8').replace(/^\uFEFF/, '');
                    
                    const parsed = Papa.parse(csvString, {
                        header: true, skipEmptyLines: 'greedy', dynamicTyping: true, transformHeader: (h) => h.trim()
                    });
                    const rowsData = parsed.data;

                    // --- BƯỚC 2: GỘP NHÓM THEO "Shipment Date" ---
                    const groupedByDate = {};
                    rowsData.forEach(row => {
                        // Tìm cột chứa chữ Shipment Date (đề phòng viết hoa/thường)
                        const dateKey = Object.keys(row).find(k => k.toLowerCase().includes('shipment date'));
                        let dateVal = dateKey && row[dateKey] ? String(row[dateKey]).trim() : 'Unknown_Date';
                        
                        // Lark Sheet cấm một số ký tự đặc biệt trong tên Tab, ta cần làm sạch
                        dateVal = dateVal.replace(/[\\/?*[\]:]/g, '-').substring(0, 31);
                        
                        if (!groupedByDate[dateVal]) groupedByDate[dateVal] = [];
                        groupedByDate[dateVal].push(row);
                    });

                    console.log(`📊 Đã chia file thành ${Object.keys(groupedByDate).length} Sheet (theo Shipment Date)`);

                    // --- BƯỚC 3: TẠO SPREADSHEET TRÊN LARK ---
                    const createSheetRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${activeToken}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ title: `Báo Cáo: ${file_name}` })
                    });
                    const sheetData = await createSheetRes.json();
                    
                    // Nếu User Token bị hết hạn, hệ thống báo lỗi -> Bạn phải thay token mới
                    if (sheetData.code !== 0) throw new Error(`Lỗi tạo Sheet: ${sheetData.msg} (Có thể Token đã hết hạn)`);
                    
                    const spreadsheetToken = sheetData.data.spreadsheet.spreadsheet_token;
                    const spreadsheetUrl = sheetData.data.spreadsheet.url;

                    // --- BƯỚC 4: TẠO CÁC TAB (SHEET) TƯƠNG ỨNG VỚI TỪNG NGÀY ---
                    const sheetNames = Object.keys(groupedByDate);
                    const tabRequests = sheetNames.map(name => ({ addSheet: { properties: { title: name } } }));
                    
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${activeToken}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    // --- BƯỚC 5: BƠM DỮ LIỆU VÀO TỪNG TAB ---
                    for (const sheetName of sheetNames) {
                        const rows = groupedByDate[sheetName];
                        if (rows.length === 0) continue;
                        
                        // Lấy danh sách tên cột
                        const headers = Object.keys(rows[0]);
                        const values = [headers]; // Dòng 1 là Tiêu đề
                        
                        // Đẩy dữ liệu từng hàng vào
                        rows.forEach(r => {
                            values.push(headers.map(h => r[h] !== null && r[h] !== undefined ? String(r[h]) : ""));
                        });

                        // Tính toán vùng dữ liệu (Range) ví dụ: "2023-10-01!A1:Z100"
                        const endColLetter = getColLetter(headers.length - 1);
                        const endRow = values.length;
                        const range = `${sheetName}!A1:${endColLetter}${endRow}`;

                        // API ghi dữ liệu vào Sheet
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${spreadsheetToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${activeToken}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: range, values: values } })
                        });
                    }

                    // --- BƯỚC 6: LƯU MONGODB & BÁO CÁO LARK ---
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData
                    });
                    const savedDoc = await newFileEntry.save();

                    // Gửi thẻ Card đính kèm Link truy cập thẳng vào Sheet vừa tạo
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ Đã Phân Tích & Tạo Sheet' }, template: "blue" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File gốc:** ${file_name}\n📊 **Số Shipment Date:** ${sheetNames.length} ngày\n🗂️ **Tổng số dòng:** ${rowsData.length}` } },
                                    {
                                        tag: 'action',
                                        actions: [
                                            { tag: 'button', text: { tag: 'plain_text', content: '🌐 Mở Lark Sheet' }, type: 'primary', url: spreadsheetUrl },
                                            { tag: 'button', text: { tag: 'plain_text', content: '🗑️ Xóa DB' }, type: 'danger', value: { action: 'delete_file', docId: savedDoc._id } }
                                        ]
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
                data: { msg_type: 'text', content: JSON.stringify({ text: `❌ Lỗi: ${error.message}` }) }
            });
        }
    }
    return res.status(200).json({ success: true });
});

// =====================================================================
// 4. WEBHOOK PHỤ (NÚT BẤM)
// =====================================================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(data.action.value.docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã xóa dữ liệu DB!' } });
        } catch (err) { return res.status(200).json({ toast: { type: 'error', content: 'Lỗi xóa bản ghi.' } }); }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;