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
const CHUNK_SIZE = 100;

function extractAttribute(row, keyword) {
    const key = Object.keys(row).find(k => k.toLowerCase().includes(keyword.toLowerCase()));
    return key && row[key] ? String(row[key]).trim() : "";
}

function chunkArray(array, size) {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) chunks.push(array.slice(i, i + size));
    return chunks;
}

function sanitizeSheetName(rawName, fallbackIndex) {
    const candidate = String(rawName || '').replace(/[\\\/\?\*\[\]\:\;]/g, '').trim().substring(0, 40);
    return candidate ? `${candidate}-${fallbackIndex + 1}` : `Invoice-${fallbackIndex + 1}`;
}

async function createSpreadsheetForBatch(tenantToken, fileName, batchIndex, rows, debugLogs) {
    const title = `${fileName.replace(/\.csv$/i, '')} - Part ${batchIndex + 1}`;
    const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ title })
    });
    const createData = await createRes.json();
    const ssToken = createData.data.spreadsheet.spreadsheet_token;
    const ssUrl = createData.data.spreadsheet.url;
    debugLogs.push(`✅ Đã tạo Spreadsheet: ${title}`);

    const sheetTitles = rows.map((row, index) => sanitizeSheetName(row.waybillNumber, index));
    const tabRequests = sheetTitles.map(sheetTitle => ({ addSheet: { properties: { title: sheetTitle } } }));
    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ requests: tabRequests })
    });

    const queryRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${tenantToken}` }
    });
    const queryData = await queryRes.json();
    const sheetIdMap = {};
    if (queryData.data && queryData.data.sheets) {
        queryData.data.sheets.forEach(s => { sheetIdMap[s.title] = s.sheet_id; });
    }

    for (let index = 0; index < rows.length; index++) {
        const row = rows[index];
        const sheetTitle = sheetTitles[index];
        const targetId = sheetIdMap[sheetTitle];
        if (!targetId) continue;

        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
            method: 'PUT',
            headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ valueRange: { range: `${targetId}!A1:F19`, values: INVOICE_TEMPLATE } })
        });

        const valueRanges = row.fields.filter(f => f.val).map(f => ({ range: `${targetId}!${f.range}`, values: [[f.val]] }));
        if (valueRanges.length > 0) {
            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_batch_update`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ valueRanges })
            });
        }

        const mergeRanges = [
            `${targetId}!A1:C4`,
            `${targetId}!A5:F5`,
            `${targetId}!A6:F6`,
            `${targetId}!A7:F7`,
            `${targetId}!A8:F8`,
            `${targetId}!A18:D18`,
            `${targetId}!A19:F19`
        ];

        const buyerName = row.fields[1].val;
        const addressTo = row.fields[2].val;
        const emailData = row.fields[3].val;
        const phoneData = row.fields[4].val;

        if (buyerName && buyerName.length > 18) mergeRanges.push(`${targetId}!B12:F12`);
        if (addressTo && (addressTo.length > 18 || addressTo.includes('\n'))) mergeRanges.push(`${targetId}!B13:F13`);
        if (emailData && emailData.length > 18) mergeRanges.push(`${targetId}!B14:F14`);
        if (phoneData && phoneData.length > 18) mergeRanges.push(`${targetId}!B15:F15`);

        for (const mRange of mergeRanges) {
            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/merge_cells`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ range: mRange, mergeType: "MERGE_ALL" })
            });
        }

        const borderLine = { style: "SOLID", color: "#000000" };
        const stylePayload = {
            data: [
                { ranges: [`${targetId}!A1:C4`], style: { hAlign: 1, vAlign: 1 } },
                { ranges: [`${targetId}!A5:F5`], style: { font: { bold: true }, hAlign: 0, vAlign: 0 } },
                { ranges: [`${targetId}!A6:F7`], style: { hAlign: 0, vAlign: 0 } },
                { ranges: [`${targetId}!A12:A15`], style: { font: { bold: true }, hAlign: 0, vAlign: 0 } },
                { ranges: [`${targetId}!B12:F15`], style: { hAlign: 0, vAlign: 0 } },
                { ranges: [`${targetId}!A8:F8`], style: { font: { bold: true }, hAlign: 1 } },
                { ranges: [`${targetId}!A18:F18`], style: { font: { bold: true } } },
                { ranges: [`${targetId}!A16:F18`], style: { border: { top: borderLine, bottom: borderLine, left: borderLine, right: borderLine, innerHorizontal: borderLine, innerVertical: borderLine } } },
                { ranges: [`${targetId}!A16:F16`], style: { font: { bold: true }, backColor: "#D9D9D9", hAlign: 1 } }
            ]
        };

        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/styles_batch_update`, {
            method: 'PUT',
            headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(stylePayload)
        });

        try {
            const logoPath = path.join(process.cwd(), 'public', 'logo.png');
            if (fs.existsSync(logoPath)) {
                const imgBuffer = fs.readFileSync(logoPath);
                const imageByteArray = Array.from(imgBuffer);
                const payload = { range: `${targetId}!A1:A1`, image: imageByteArray, name: 'logo.png' };
                await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_image`, {
                    method: 'POST',
                    headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
            }
        } catch (imgErr) {
            debugLogs.push(`❌ Lỗi logo [${sheetTitle}]: ${imgErr.message}`);
        }
    }

    return { title, url: ssUrl, rowCount: rows.length };
}

// 📝 TEMPLATE ĐÃ CHUẨN HÓA (Giá mặc định 30)
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
    ["1", "", "Pair", "30", "", "0.0"], // <--- Set cứng 30
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
                        const englishName = rawDesc.trim();
                        const vietnameseName = translateProductName(englishName);
                        const ProductName = vietnameseName ? `${englishName}\n${vietnameseName}` : englishName;

                        // 🛠 TÍNH TOÁN: Lấy Quantity * 30
                        const numericQty = parseFloat(qtyVal) || 1;
                        const totalAmount = (30 * numericQty).toFixed(1); 

                        return {
                            waybillNumber: extractAttribute(row, 'waybill number') || `WB_${Math.floor(Math.random()*1000)}`,
                            fields: [
                                { val: shipmentDate, range: "F10:F10" },
                                { val: extractAttribute(row, 'recipient name'), range: "B12:B12" },
                                { val: fullAddress, range: "B13:B13" },
                                { val: extractAttribute(row, 'email'), range: "B14:B14" },
                                { val: extractAttribute(row, 'recipient phone'), range: "B15:B15" },
                                { val: ProductName, range: "B17:B17" }, 
                                // Bắn xuống dòng Sản Phẩm (Dòng 17)
                                { val: qtyVal, range: "E17:E17" },           
                                { val: totalAmount, range: "F17:F17" },      
                                // Bắn luôn kết quả chốt hạ xuống dòng Total (Dòng 18)
                                { val: qtyVal, range: "E18:E18" },           
                                { val: totalAmount, range: "F18:F18" }       
                            ]
                        };
                    });

                    const rowChunks = chunkArray(rowsData, CHUNK_SIZE);
                    const createdSheets = [];

                    for (let batchIndex = 0; batchIndex < rowChunks.length; batchIndex++) {
                        const batchRows = rowChunks[batchIndex];
                        debugLogs.push(`📦 Bắt đầu batch ${batchIndex + 1}/${rowChunks.length} với ${batchRows.length} invoices`);
                        const batchResult = await createSpreadsheetForBatch(tenantToken, `Invoices: ${file_name}`, batchIndex, batchRows, debugLogs);
                        createdSheets.push(batchResult);
                    }

                    await connectDB();
                    await (new CsvVault({ fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData })).save();

                    const actions = createdSheets.map(sheet => ({
                        tag: 'button',
                        text: { tag: 'plain_text', content: `${sheet.title} (${sheet.rowCount})` },
                        type: 'primary',
                        url: sheet.url
                    }));

                    const finalLogText = debugLogs.join('\n').substring(0, 3000);

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ TẠO HÓA ĐƠN HOÀN TẤT' }, template: "blue" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số Invoice:** ${rowsData.length}\n📄 **Số Files:** ${createdSheets.length}` } },
                                    { tag: 'hr' },
                                    { tag: 'action', actions: actions.slice(0, 5) } ,
                                    { tag: 'div', text: { tag: 'lark_md', content: `**🖥️ VERCEL DEBUG LOGS:**\n\`\`\`\n${finalLogText}\n\`\`\`` } }
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