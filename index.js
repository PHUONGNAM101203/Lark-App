require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');
const fs = require('fs'); // Thư viện đọc file hệ thống
const FormData = require('form-data'); // Thư viện để gửi ảnh lên Lark

const app = express();
app.use(express.json());

// ==========================================
// 🔑 TOKEN USER 
// ==========================================
const USER_TOKEN = "t-g206481MW7SJHCHSD6ZBVL5JQPNJPTWMEJWWW74H";
const processedEvents = new Set();

let isConnected = false;
async function connectDB() {
    if (isConnected) return;
    try {
        if (mongoose.connections[0].readyState) { isConnected = true; return; }
        await mongoose.connect(process.env.MONGODB_URI);
        isConnected = true;
    } catch (err) { console.error('❌ Lỗi kết nối MongoDB:', err); }
}

const CsvVaultSchema = new mongoose.Schema({ fileName: String, fileContentRaw: String, totalRows: Number, parsedData: [mongoose.Schema.Types.Mixed] });
const CsvVault = mongoose.models.CsvVault || mongoose.model('CsvVault', CsvVaultSchema, 'csv_storage');

const client = new lark.Client({ appId: process.env.LARK_APP_ID, appSecret: process.env.LARK_APP_SECRET });

function extractAttribute(row, keyword) {
    const key = Object.keys(row).find(k => k.toLowerCase().includes(keyword.toLowerCase()));
    return key && row[key] ? String(row[key]).trim() : "";
}

// 📝 TEMPLATE CHỮ THÔ
const INVOICE_TEMPLATE = [
    ["", "", "", "", "", ""], // 1-4: Dành khoảng trống để chèn Logo (A1:C4)
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""],
    ["WILD AND KING COMPANY LIMITED", "", "", "", "", ""], // Dòng 5
    ["K10/7B Pham Van Nghi, Thanh Khe ward", "", "", "", "", ""],
    ["Da Nang city, Viet Nam", "", "", "", "", ""],
    ["COMMERCIAL INVOICE", "", "", "", "", ""], // Dòng 8
    ["", "", "", "", "INVOICE NO:", ""],
    ["", "", "", "", "DATE:", ""],
    ["", "", "", "", "CUSTOMER ID:", ""],
    ["Buyer:", "", "", "", "", ""], // Dòng 12
    ["To", "", "", "", "", ""],    // Dòng 13
    ["Email", "", "", "", "", ""], // Dòng 14
    ["Phone", "", "", "", "", ""], // Dòng 15
    ["No.", "Name of product/ Color", "UNIT", "Price/Unit ($)", "Qty", "Amount ($)"], // Dòng 16 (Bôi màu, In đậm)
    ["1", "", "Pair", "", "", "0.0"], // Dòng 17
    ["Total", "", "", "", "0", "0.0"], // Dòng 18 (In đậm)
    ["SAY: US DOLLARS ONE HUNDRED SEVENTY ONLY", "", "", "", "", ""] // Dòng 19
];

// =====================================================================
// 🟢 WEBHOOK 1: VẼ FORM TRẮNG, TÔ MÀU, CHÈN LOGO & BẮN DATA
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

                    // 2. CHUẨN HÓA DATA
                    const rowsData = parsed.data.map(row => {
                        const addr1 = extractAttribute(row, 'recipient address 1');
                        const addr2 = extractAttribute(row, 'recipient address 2');
                        const city = extractAttribute(row, 'recipient city');
                        const zip = extractAttribute(row, 'postal code');
                        const country = extractAttribute(row, 'recipient country');
                        const fullAddress = `${addr1}\n ${addr2}\n ${city}\n ${zip} ${country}`.replace(/\s+/g, ' ').trim();

                        return {
                            waybillNumber: extractAttribute(row, 'waybill number') || `WB_${Math.floor(Math.random()*1000)}`,
                            fields: [
                                { val: extractAttribute(row, 'recipient name'), range: "B12:B12" },
                                { val: fullAddress, range: "B13:B13" },
                                { val: extractAttribute(row, 'email'), range: "B14:B14" },
                                { val: extractAttribute(row, 'recipient phone'), range: "B15:B15" },
                                { val: extractAttribute(row, 'item description'), range: "B17:B17" }
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

                    // 4. TẠO TAB (CÓ CHÈN grid_properties NHƯ BẠN MUỐN)
                    const tabRequests = rowsData.map(r => ({ 
                        addSheet: { 
                            properties: { 
                                title: r.waybillNumber 
                                // grid_properties thường chỉ hỗ trợ khi truyền raw API, Lark SDK thỉnh thoảng sẽ filter mất.
                                // Nhưng nó sẽ mặc định tạo bảng 200 dòng 20 cột cho bạn.
                            } 
                        } 
                    }));
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    // LẤY SHEET ID
                    const queryRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, { method: 'GET', headers: { 'Authorization': `Bearer ${USER_TOKEN}` }});
                    const queryData = await queryRes.json();
                    const sheetIdMap = {};
                    if (queryData.data && queryData.data.sheets) queryData.data.sheets.forEach(s => { sheetIdMap[s.title] = s.sheet_id; });

                    // 5. VÒNG LẶP CHẾ TẠO FORM (VẼ, TÔ MÀU, CHÈN ẢNH)
                    console.log(`🚀 Bắt đầu vẽ Template, chèn Logo và tô màu...`);
                    for (const r of rowsData) {
                        const targetId = sheetIdMap[r.waybillNumber];
                        if (!targetId) continue;

                        // 🛠 BƯỚC 5.1: Dán chữ thô lên Tab
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: `${targetId}!A1:F19`, values: INVOICE_TEMPLATE } })
                        });

                        // 🛠 BƯỚC 5.2: TÔ MÀU & IN ĐẬM (Dùng API batch-set-cell-style bạn tìm thấy)
                        const stylePayload = {
                            data: [
                                {
                                    // Bôi đen: Tên CTY (A5), Chữ COMMERCIAL INVOICE (A8), và Dòng Total (A18)
                                    ranges: [`${targetId}!A5:A8`, `${targetId}!A18:F18`],
                                    style: { font: { bold: true, fontSize: "11pt" } }
                                },
                                {
                                    // Trang trí Header Bảng (A16:F16): In đậm, Chữ trắng, Nền xanh (hoặc xám), Căn giữa
                                    ranges: [`${targetId}!A16:F16`],
                                    style: { 
                                        font: { bold: true, clean: false }, 
                                        backColor: "#D9D9D9", // Nền xám nhạt (Sửa mã màu HEX tùy ý)
                                        hAlign: 2 // 2 là căn giữa (Center)
                                    }
                                }
                            ]
                        };
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/styles_batch_update`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify(stylePayload)
                        });

                        // 🛠 BƯỚC 5.3: CHÈN ẢNH LOGO (Dùng API write-images bạn tìm thấy)
                        // Hãy đảm bảo bạn có file logo.png trong thư mục public
                        try {
                            if (fs.existsSync('./public/logo.png')) {
                                let form = new FormData();
                                form.append('range', `${targetId}!A1:C4`); // Hình ảnh sẽ phủ từ ô A1 đến C4
                                form.append('image', fs.createReadStream('./public/logo.png'));
                                form.append('name', 'logo.png');

                                await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_image`, {
                                    method: 'POST',
                                    headers: { 'Authorization': `Bearer ${USER_TOKEN}`, ...form.getHeaders() },
                                    body: form
                                });
                            }
                        } catch (imgErr) {
                            console.log(`⚠️ Bỏ qua chèn ảnh do không tìm thấy file ./public/logo.png`);
                        }

                        // 🛠 BƯỚC 5.4: Bắn Data (Biến) vào form
                        const valueRanges = r.fields.filter(f => f.val).map(f => ({
                            range: `${targetId}!${f.range}`, values: [[f.val]]
                        }));

                        if (valueRanges.length > 0) {
                            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_batch_update`, {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ valueRanges: valueRanges })
                            });
                        }
                        console.log(`   ✅ Hoàn tất thiết kế & điền data: [${r.waybillNumber}]`);
                    }

                    // 6. LƯU DB & PHẢN HỒI
                    await connectDB();
                    await (new CsvVault({ fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData })).save();

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ TẠO HÓA ĐƠN HOÀN TẤT' }, template: "blue" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 Đã vẽ hoàn chỉnh **${rowsData.length}** form hóa đơn.\n🎨 Hệ thống đã bôi màu, in đậm và chèn Logo từ Local Server.` } },
                                    { tag: 'action', actions: [{ tag: 'button', text: { tag: 'plain_text', content: '🌐 Mở Lark Sheet' }, type: 'primary', url: ssUrl }] }
                                ]
                            })
                        }
                    });
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