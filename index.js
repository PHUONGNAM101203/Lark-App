require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');
const { getCountryName } = require('./countryCodes');
const fs = require('fs');

const app = express();
app.use(express.json());

// ==========================================
// 🔑 TOKEN USER 
// ==========================================
const USER_TOKEN = "t-g206483J6SFYUPBMPKEAEZYQYIR5HC66OA5LKATR";
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
    fileName: String, fileContentRaw: String, totalRows: Number, parsedData: [mongoose.Schema.Types.Mixed], importedAt: { type: Date, default: Date.now }
});
const CsvVault = mongoose.models.CsvVault || mongoose.model('CsvVault', CsvVaultSchema, 'csv_storage');

const client = new lark.Client({ appId: process.env.LARK_APP_ID, appSecret: process.env.LARK_APP_SECRET });

// Hàm quét từ khóa
function extractAttribute(row, keyword) {
    const key = Object.keys(row).find(k => k.toLowerCase().includes(keyword.toLowerCase()));
    return key && row[key] ? String(row[key]).trim() : "";
}

// =====================================================================
// 📝 TEMPLATE HÓA ĐƠN CỐ ĐỊNH
// =====================================================================
const INVOICE_TEMPLATE = [
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""], // A1:C4 sẽ dùng để chèn LOGO
    ["WILD AND KING COMPANY LIMITED", "", "", "", "", ""],
    ["K10/7B Pham Van Nghi, Thanh Khe ward", "", "", "", "", ""],
    ["Da Nang city, Viet Nam", "", "", "", "", ""],
    ["COMMERCIAL INVOICE", "", "", "", "", ""],
    ["", "", "", "", "INVOICE NO:", ""],
    ["", "", "", "", "DATE:", ""], // Dòng 10 (Index 9)
    ["", "", "", "", "CUSTOMER ID:", ""],
    ["Buyer:", "", "", "", "", ""], // Dòng 12
    ["To", "", "", "", "", ""],    // Dòng 13
    ["Email", "", "", "", "", ""], // Dòng 14
    ["Phone", "", "", "", "", ""], // Dòng 15
    ["No.", "Name of product/ Color", "UNIT", "Price/Unit ($)", "Qty", "Amount ($)"], // Dòng 16 (Header)
    ["1", "", "Pair", "", "", "0.0"], // Dòng 17
    ["Total", "", "", "", "0", "0.0"], // Dòng 18
    ["SAY: US DOLLARS ONE HUNDRED SEVENTY ONLY", "", "", "", "", ""] // Dòng 19
];

// =====================================================================
// 🟢 WEBHOOK 1: VẼ FORM, IN ĐẬM, TÔ MÀU, KẺ BẢNG, GỘP Ô, LỌC SỐ
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
                    console.log(`📂 BẮT ĐẦU TẠO HÓA ĐƠN TỪ FILE: ${file_name}`);

                    // 1. TẢI FILE
                    const tokenRes = await client.auth.tenantAccessToken.internal({ data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET }});
                    const tenantToken = tokenRes.tenant_access_token;
                    
                    const fetchRes = await fetch(`https://open.larksuite.com/open-apis/im/v1/messages/${message.message_id}/resources/${file_key}?type=file`, { headers: { 'Authorization': `Bearer ${tenantToken}` } });
                    const fileBuffer = Buffer.from(await fetchRes.arrayBuffer());
                    let csvString = fileBuffer.toString('utf-8').replace(/^\uFEFF/, '');
                    const parsed = Papa.parse(csvString, { header: true, skipEmptyLines: 'greedy', dynamicTyping: true, transformHeader: (h) => h.trim() });

                    // 2. CHUẨN HÓA DATA & XỬ LÝ SỐ QUANTITY
                    const rowsData = parsed.data.map(row => {
                        const addr1 = extractAttribute(row, 'recipient address 1');
                        const addr2 = extractAttribute(row, 'recipient address 2');
                        const city = extractAttribute(row, 'recipient city');
                        const zip = extractAttribute(row, 'postal code');
                        
                        // ✅ ĐÃ FIX LỖI TRÙNG BIẾN (Gộp tìm kiếm vào 1 dòng duy nhất)
                        const rawCountry = extractAttribute(row, 'recipient country') || extractAttribute(row, 'country');
                        const country = rawCountry ? getCountryName(rawCountry) : "";
                        
                        const fullAddress = [addr1, addr2, city, `${zip} ${country}`.trim()].filter(Boolean).join('\n');

                        // LẤY SHIPMENT DATE
                        const shipmentDate = extractAttribute(row, 'shipment date') || "";

                        // LỌC QUANTITY
                        const rawDesc = extractAttribute(row, 'item description') || "";
                        const qtyMatch = rawDesc.match(/^(\d+(\.\d+)?)/); 
                        const qtyVal = qtyMatch ? qtyMatch[0] : "1"; 
                        const cleanProductName = rawDesc.replace(/^(\d+(\.\d+)?)\s*/, '').trim();

                        return {
                            waybillNumber: extractAttribute(row, 'waybill number') || `WB_${Math.floor(Math.random()*1000)}`,
                            fields: [
                                { val: shipmentDate, range: "F10:F10" },            // ✅ Gắn Date vào vị trí F10
                                { val: extractAttribute(row, 'recipient name'), range: "B12:B12" },
                                { val: fullAddress, range: "B13:B13" },
                                { val: extractAttribute(row, 'email'), range: "B14:B14" },
                                { val: extractAttribute(row, 'recipient phone'), range: "B15:B15" },
                                { val: cleanProductName, range: "B17:B17" }, 
                                { val: qtyVal, range: "E17:E17" }           
                            ]
                        };
                    });

                    // 3. TẠO SPREADSHEET TỔNG
                    console.log(`📝 Đang tạo Spreadsheet Invoices...`);
                    const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ title: `Invoices: ${file_name}` })
                    });
                    const createData = await createRes.json();
                    const ssToken = createData.data.spreadsheet.spreadsheet_token;
                    const ssUrl = createData.data.spreadsheet.url;

                    // 4. TẠO TAB CHO TỪNG ĐƠN HÀNG
                    const tabRequests = rowsData.map(r => ({ addSheet: { properties: { title: r.waybillNumber } } }));
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    const queryRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, { method: 'GET', headers: { 'Authorization': `Bearer ${USER_TOKEN}` }});
                    const queryData = await queryRes.json();
                    const sheetIdMap = {};
                    if (queryData.data && queryData.data.sheets) {
                        queryData.data.sheets.forEach(s => { sheetIdMap[s.title] = s.sheet_id; });
                    }

                    // 5. VẼ FORM, TÔ MÀU, KẺ BẢNG, GỘP Ô, CHÈN LOGO
                    console.log(`🚀 Bắt đầu Trang trí Template (Size 13, Kẻ Bảng, Gộp Ô)...`);
                    for (const r of rowsData) {
                        const targetId = sheetIdMap[r.waybillNumber];
                        if (!targetId) continue;

                        // 🛠 BƯỚC 5A: Dán Template chữ thô
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: `${targetId}!A1:F19`, values: INVOICE_TEMPLATE } })
                        });

                        // 🛠 BƯỚC 5B: GỘP Ô (Merge Cells) CHỐNG TRÀN CHỮ
                        const mergeRanges = [
                            `${targetId}!A8:F8`,   // COMMERCIAL INVOICE
                            `${targetId}!A18:D18`, // Total
                            `${targetId}!A19:F19`, // SAY: US DOLLARS...
                            `${targetId}!B12:F12`, // Buyer Name
                            `${targetId}!B13:F13`, // Buyer Address
                            `${targetId}!B14:F14`, // Email
                            `${targetId}!B15:F15`  // Phone
                        ];
                        for (const mRange of mergeRanges) {
                            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/merge_cells`, {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ range: mRange, mergeType: "MERGE_ALL" })
                            });
                        }

                        // 🛠 BƯỚC 5C: ĐỊNH DẠNG STYLE & KẺ BẢNG
                        const borderLine = { style: "SOLID", color: "#000000" };
                        const stylePayload = {
                            data: [
                                {   // 1. Ép toàn bộ tờ giấy về Font Size 13pt
                                    ranges: [`${targetId}!A1:F20`],
                                    style: { font: { fontSize: "13pt" } }
                                },
                                {   // 2. In đậm & Căn giữa chữ COMMERCIAL INVOICE
                                    ranges: [`${targetId}!A8:A8`],
                                    style: { font: { bold: true, fontSize: "13pt" }, hAlign: 2 }
                                },
                                {   // 3. In đậm Tên cty và dòng Total
                                    ranges: [`${targetId}!A5:A5`, `${targetId}!A18:A18`],
                                    style: { font: { bold: true, fontSize: "13pt" } }
                                },
                                {   // 4. ✅ KẺ BẢNG (Borders) xung quanh khu vực Sản phẩm
                                    ranges: [`${targetId}!A16:F18`],
                                    style: { border: { top: borderLine, bottom: borderLine, left: borderLine, right: borderLine } }
                                },
                                {   // 5. Bôi màu nền xám, in đậm cho Thanh Header Bảng
                                    ranges: [`${targetId}!A16:F16`],
                                    style: { font: { bold: true, fontSize: "13pt" }, backColor: "#D9D9D9", hAlign: 2 }
                                }
                            ]
                        };
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/styles_batch_update`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify(stylePayload)
                        });

                        // 🛠 BƯỚC 5D: CHÈN LOGO TỪ FOLDER PUBLIC 
                        try {
                            if (fs.existsSync('./public/logo.png')) {
                                const imgBuffer = fs.readFileSync('./public/logo.png');
                                const imgBlob = new Blob([imgBuffer], { type: 'image/png' });
                                
                                const form = new FormData();
                                form.append('range', `${targetId}!A1:C4`); 
                                form.append('image', imgBlob, 'logo.png');
                                form.append('name', 'logo.png');

                                await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_image`, {
                                    method: 'POST',
                                    headers: { 'Authorization': `Bearer ${USER_TOKEN}` }, 
                                    body: form
                                });
                            }
                        } catch (imgErr) {
                            console.error(`   ❌ Lỗi chèn ảnh:`, imgErr.message);
                        }

                        // 🛠 BƯỚC 5E: Bắn các biến (Tên, Địa chỉ, Date, Qty...) vào form
                        const valueRanges = r.fields.filter(f => f.val).map(f => ({
                            range: `${targetId}!${f.range}`, values: [[f.val]]
                        }));

                        if (valueRanges.length > 0) {
                            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_batch_update`, {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ valueRanges: valueRanges })
                            });
                            console.log(`   ✅ Hoàn tất hóa đơn: [${r.waybillNumber}]`);
                        }
                    }

                    // 6. LƯU DB & PHẢN HỒI
                    await connectDB();
                    await (new CsvVault({ fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData })).save();

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ TẠO HÓA ĐƠN HOÀN TẤT' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số lượng Invoice:** ${rowsData.length}\n🚀 Đã áp dụng Size 13, **Kẻ Bảng**, **Gộp Ô (Merge)**, và gắn **Shipment Date**!` } },
                                    {
                                        tag: 'action',
                                        actions: [
                                            { tag: 'button', text: { tag: 'plain_text', content: '🌐 Mở Lark Sheet' }, type: 'primary', url: ssUrl }
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
            console.error("❌ LỖI HỆ THỐNG:", error.message);
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;
if (process.env.NODE_ENV !== 'production') app.listen(3000, () => console.log(`🚀 Server running!`));