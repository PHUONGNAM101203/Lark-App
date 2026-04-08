require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');
const { getCountryName } = require('./countryCodes');
const { translateProductName } = require('./translations');
const fs = require('fs');
const path = require('path'); 

const app = express();
app.use(express.json());

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

const CsvVaultSchema = new mongoose.Schema({
    fileName: String, fileContentRaw: String, totalRows: Number, parsedData: [mongoose.Schema.Types.Mixed], importedAt: { type: Date, default: Date.now }
});
const CsvVault = mongoose.models.CsvVault || mongoose.model('CsvVault', CsvVaultSchema, 'csv_storage');

const client = new lark.Client({ appId: process.env.LARK_APP_ID, appSecret: process.env.LARK_APP_SECRET });

function extractAttribute(row, keyword) {
    const key = Object.keys(row).find(k => k.toLowerCase().includes(keyword.toLowerCase()));
    return key && row[key] ? String(row[key]).trim() : "";
}

const INVOICE_TEMPLATE = [
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""],
    ["", "", "", "", "", ""], 
    ["WILD AND KING COMPANY LIMITED", "", "", "", "", ""],
    ["K10/7B Pham Van Nghi, Thanh Khe ward", "", "", "", "", ""],
    ["Da Nang city, Viet Nam", "", "", "", "", ""],
    ["COMMERCIAL INVOICE", "", "", "", "", ""],
    ["", "", "", "", "INVOICE NO:", ""],
    ["", "", "", "", "DATE:", ""], 
    ["", "", "", "", "CUSTOMER ID:", ""],
    ["Buyer:", "", "", "", "", ""], 
    ["To", "", "", "", "", ""],    
    ["Email", "", "", "", "", ""], 
    ["Phone", "", "", "", "", ""], 
    ["No.", "Name of product/ Color", "UNIT", "Price/Unit ($)", "Qty", "Amount ($)"], 
    ["1", "", "Pair", "", "", "0.0"], 
    ["Total", "", "", "", "0", "0.0"], 
    ["SAY: US DOLLARS ONE HUNDRED SEVENTY ONLY", "", "", "", "", ""] 
];

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
                    let debugLogs = [];
                    debugLogs.push(`🚀 Bắt đầu xử lý file: ${file_name}`);

                    const tokenRes = await client.auth.tenantAccessToken.internal({ data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET }});
                    const tenantToken = tokenRes.tenant_access_token;
                    
                    const fetchRes = await fetch(`https://open.larksuite.com/open-apis/im/v1/messages/${message.message_id}/resources/${file_key}?type=file`, { headers: { 'Authorization': `Bearer ${tenantToken}` } });
                    const fileBuffer = Buffer.from(await fetchRes.arrayBuffer());
                    let csvString = fileBuffer.toString('utf-8').replace(/^\uFEFF/, '');
                    const parsed = Papa.parse(csvString, { header: true, skipEmptyLines: 'greedy', dynamicTyping: true, transformHeader: (h) => h.trim() });

                    const rowsData = parsed.data.map(row => {
                        const addr1 = extractAttribute(row, 'recipient address 1');
                        const addr2 = extractAttribute(row, 'recipient address 2');
                        const city = extractAttribute(row, 'recipient city');
                        const zip = extractAttribute(row, 'postal code');
                        const rawCountry = extractAttribute(row, 'recipient country') || extractAttribute(row, 'country');
                        const country = rawCountry ? getCountryName(rawCountry) : "";
                        const fullAddress = [addr1, addr2, city, `${zip} ${country}`.trim()].filter(Boolean).join('\n');
                        const shipmentDate = extractAttribute(row, 'shipment date') || "";
                        const rawDesc = extractAttribute(row, 'item description') || "";
                        const qtyMatch = rawDesc.match(/^(\d+(\.\d+)?)/); 
                        const qtyVal = qtyMatch ? qtyMatch[0] : "1"; 
                        const englishName = rawDesc.replace(/^(\d+(\.\d+)?)\s*/, '').trim();
                        const vietnameseName = translateProductName(englishName);
                        const ProductName = vietnameseName ? `${englishName}\n${vietnameseName}` : englishName;

                        return {
                            waybillNumber: extractAttribute(row, 'waybill number') || `WB_${Math.floor(Math.random()*1000)}`,
                            fields: [
                                { val: shipmentDate, range: "F10:F10" },
                                { val: extractAttribute(row, 'recipient name'), range: "B12:B12" },
                                { val: fullAddress, range: "B13:B13" },
                                { val: extractAttribute(row, 'email'), range: "B14:B14" },
                                { val: extractAttribute(row, 'recipient phone'), range: "B15:B15" },
                                { val: ProductName, range: "B17:B17" }, 
                                { val: qtyVal, range: "E17:E17" }           
                            ]
                        };
                    });

                    const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ title: `Invoices: ${file_name}` })
                    });
                    const createData = await createRes.json();
                    const ssToken = createData.data.spreadsheet.spreadsheet_token;
                    const ssUrl = createData.data.spreadsheet.url;
                    debugLogs.push(`✅ Đã tạo Spreadsheet thành công.`);

                    const tabRequests = rowsData.map(r => ({ addSheet: { properties: { title: r.waybillNumber } } }));
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    const queryRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, { method: 'GET', headers: { 'Authorization': `Bearer ${tenantToken}` }});
                    const queryData = await queryRes.json();
                    const sheetIdMap = {};
                    if (queryData.data && queryData.data.sheets) {
                        queryData.data.sheets.forEach(s => { sheetIdMap[s.title] = s.sheet_id; });
                    }

                    for (const r of rowsData) {
                        const targetId = sheetIdMap[r.waybillNumber];
                        if (!targetId) continue;

                        // 🛠 1. Dán Template chữ thô
                        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: `${targetId}!A1:F19`, values: INVOICE_TEMPLATE } })
                        });

                        // 🛠 2. ĐỊNH DẠNG STYLE (BỎ ÉP FONT, GIỮ LẠI TẤT CẢ CÁC STYLE KHÁC)
                        const borderLine = { style: "SOLID", color: "#000000" };
                        const stylePayload = {
                            data: [
                                // Căn Giữa & Giữa cho ô Logo để ảnh nằm ngay ngắn
                                { ranges: [`${targetId}!A1:C4`], style: { hAlign: 2, vAlign: 1 } },
                                // In đậm & Căn giữa COMMERCIAL INVOICE
                                { ranges: [`${targetId}!A8:F8`], style: { font: { bold: true }, hAlign: 2 } },
                                // In đậm tên Cty và dòng Total
                                { ranges: [`${targetId}!A5:F5`, `${targetId}!A18:F18`], style: { font: { bold: true } } },
                                // Căn Trái & Trên cho cụm Thông tin Khách hàng (Buyer -> Phone)
                                { ranges: [`${targetId}!B12:F15`], style: { hAlign: 1, vAlign: 0 } },
                                // Kẻ bảng xung quanh Sản phẩm
                                { ranges: [`${targetId}!A16:F18`], style: { border: { top: borderLine, bottom: borderLine, left: borderLine, right: borderLine, innerHorizontal: borderLine, innerVertical: borderLine } } },
                                // Đổ màu nền Xám, In đậm, Căn giữa cho Header Bảng
                                { ranges: [`${targetId}!A16:F16`], style: { font: { bold: true }, backColor: "#D9D9D9", hAlign: 2 } }
                            ]
                        };
                        
                        const styleRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/styles_batch_update`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify(stylePayload)
                        });
                        const styleLog = await styleRes.json();
                        if(styleLog.code !== 0) {
                            debugLogs.push(`❌ Lỗi Style [${r.waybillNumber}]: ${styleLog.msg}`);
                        } else {
                            debugLogs.push(`✅ Đã áp Style cho [${r.waybillNumber}]`);
                        }

                        // 🛠 3. GỘP Ô (Merge Cells) SAU KHI ĐÃ STYLE 
                        const mergeRanges = [
                            `${targetId}!A1:C4`, // Gộp ô Logo
                            `${targetId}!A8:F8`, `${targetId}!A18:D18`, `${targetId}!A19:F19`, 
                            `${targetId}!B12:F12`, `${targetId}!B13:F13`, `${targetId}!B14:F14`, `${targetId}!B15:F15`
                        ];
                        for (const mRange of mergeRanges) {
                            const mergeRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/merge_cells`, {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ range: mRange, mergeType: "MERGE_ALL" })
                            });
                            const mergeLog = await mergeRes.json();
                            if(mergeLog.code !== 0) debugLogs.push(`❌ Lỗi Merge ${mRange}: ${mergeLog.msg}`);
                        }

                        // 🛠 4. CHÈN LOGO
                        try {
                            const logoPath = path.join(process.cwd(), 'public', 'logo.png');
                            if (fs.existsSync(logoPath)) {
                                const imgBuffer = fs.readFileSync(logoPath);
                                const imageByteArray = Array.from(imgBuffer);
                                
                                const payload = {
                                    range: `${targetId}!A1:A1`, 
                                    image: imageByteArray,
                                    name: 'logo.png'
                                };
                                
                                const imgRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_image`, {
                                    method: 'POST',
                                    headers: { 
                                        'Authorization': `Bearer ${tenantToken}`,
                                        'Content-Type': 'application/json' 
                                    }, 
                                    body: JSON.stringify(payload)
                                });
                                
                                const imgLog = await imgRes.json();
                                if(imgLog.code !== 0) {
                                    debugLogs.push(`❌ Lỗi Logo [${r.waybillNumber}]: ${imgLog.msg}`);
                                } else {
                                    debugLogs.push(`✅ Đã chèn Logo cho [${r.waybillNumber}]`);
                                }
                            } else {
                                debugLogs.push(`⚠️ Không tìm thấy file Logo trên Vercel tại: ${logoPath}`);
                            }
                        } catch (imgErr) {
                            debugLogs.push(`❌ Lỗi catch Logo [${r.waybillNumber}]: ${imgErr.message}`);
                        }

                        // 🛠 5. ĐIỀN DỮ LIỆU
                        const valueRanges = r.fields.filter(f => f.val).map(f => ({
                            range: `${targetId}!${f.range}`, values: [[f.val]]
                        }));

                        if (valueRanges.length > 0) {
                            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_batch_update`, {
                                method: 'POST',
                                headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ valueRanges: valueRanges })
                            });
                        }
                    }

                    await connectDB();
                    await (new CsvVault({ fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData })).save();

                    const finalLogText = debugLogs.join('\n').substring(0, 3000); 

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ TẠO HÓA ĐƠN HOÀN TẤT' }, template: "blue" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số Invoice:** ${rowsData.length}` } },
                                    { tag: 'hr' },
                                    { tag: 'div', text: { tag: 'lark_md', content: `**🖥️ VERCEL DEBUG LOGS:**\n\`\`\`\n${finalLogText}\n\`\`\`` } },
                                    { tag: 'action', actions: [{ tag: 'button', text: { tag: 'plain_text', content: '🌐 Mở Lark Sheet' }, type: 'primary', url: ssUrl }] }
                                ]
                            })
                        }
                    });
                }
            }
        } catch (error) {
            await client.im.message.reply({
                path: { message_id: message.message_id },
                data: {
                    msg_type: 'text',
                    content: JSON.stringify({ text: `❌ LỖI HỆ THỐNG (Vercel Crash):\n${error.message}\n${error.stack}` })
                }
            });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;
if (process.env.NODE_ENV !== 'production') app.listen(3000, () => console.log(`🚀 Server running!`));