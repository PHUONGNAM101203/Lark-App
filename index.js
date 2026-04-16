require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');
const { getCountryName } = require('./countryCodes');
const { translateProductName, cleanProductKey, getProductUnit } = require('./translations');
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
const CHUNK_SIZE = 70;

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
    return candidate ? `${candidate}-${fallbackIndex + 2}` : `Invoice-${fallbackIndex + 2}`;
}

// 📝 TEMPLATE ĐÃ CHUẨN HÓA (Giá mặc định 30)
const INVOICE_TEMPLATE = [
    ["", "", "", "", "", ""], ["", "", "", "", "", ""], ["", "", "", "", "", ""], ["", "", "", "", "", ""],
    ["WILD AND KING COMPANY LIMITED", "", "", "", "", ""],
    ["K10/7B Pham Van Nghi, Thanh Khe ward", "", "", "", "", ""],
    ["Da Nang city, Viet Nam", "", "", "", "", ""],
    ["COMMERCIAL INVOICE", "", "", "", "", ""],
    ["", "", "", "", "INVOICE NO:", ""], ["", "", "", "", "DATE:", ""], ["", "", "", "", "CUSTOMER ID:", ""],
    ["Buyer:", "", "", "", "", ""], ["To", "", "", "", "", ""], ["Email", "", "", "", "", ""], ["Phone", "", "", "", "", ""],
    ["No.", "Name of product/ Color", "UNIT", "Price/Unit ($)", "Qty", "Amount ($)"],
    ["1", "", "", "30", "", "0.0"],
    ["Total", "", "", "", "0", "0.0"],
    ["SAY: US DOLLARS ONE HUNDRED SEVENTY ONLY", "", "", "", "", ""]
];

// =========================================================================
// 🔥 THUẬT TOÁN ĐÃ FIX: CHIA NHỎ REQUEST COPY + BẮT LỖI CHẶT CHẼ
// =========================================================================
async function createSpreadsheetForBatch(tenantToken, fileName, batchIndex, rows, debugLogs) {
    const title = `${fileName.replace(/\.csv$/i, '')} - Part ${batchIndex + 1}`;
    debugLogs.push(`🔄 Bắt đầu tạo file: ${title}`);

    // 1. TẠO FILE SPREADSHEET MỚI
    const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ title })
    });
    const createData = await createRes.json();
    const ssToken = createData.data.spreadsheet.spreadsheet_token;
    const ssUrl = createData.data.spreadsheet.url;

    // 2. LẤY ID CỦA SHEET MẶC ĐỊNH LÀM TEMPLATE GỐC
    const queryRes1 = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, {
        method: 'GET', headers: { 'Authorization': `Bearer ${tenantToken}` }
    });
    const queryData1 = await queryRes1.json();
    const templateSheetId = queryData1.data.sheets[0].sheet_id;

    // 3. CHỈ VẼ FORM, GỘP Ô VÀ CHÈN LOGO 1 LẦN DUY NHẤT LÊN TEMPLATE NÀY
    // A. Values
    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
        method: 'PUT',
        headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ valueRange: { range: `${templateSheetId}!A1:F19`, values: INVOICE_TEMPLATE } })
    });

    // B. Merge (Gộp luôn các ô Info như Buyer, To, Email dãn dài ra F, không cần if-else)
    const mergeRanges = [
        `${templateSheetId}!A1:C4`, `${templateSheetId}!A5:F5`, `${templateSheetId}!A6:F6`,
        `${templateSheetId}!A7:F7`, `${templateSheetId}!A8:F8`, `${templateSheetId}!A18:D18`,
        `${templateSheetId}!A19:F19`, `${templateSheetId}!B12:F12`, `${templateSheetId}!B13:F13`,
        `${templateSheetId}!B14:F14`, `${templateSheetId}!B15:F15`
    ];
    for (const mRange of mergeRanges) {
        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/merge_cells`, {
            method: 'POST', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ range: mRange, mergeType: "MERGE_ALL" })
        });
    }

    // C. Style
    const stylePayload = {
        data: [
            { ranges: [`${templateSheetId}!A1:C4`], style: { hAlign: 1, vAlign: 1 } },
            { ranges: [`${templateSheetId}!A5:F5`], style: { font: { bold: true }, hAlign: 0, vAlign: 0 } },
            { ranges: [`${templateSheetId}!A6:F7`], style: { hAlign: 0, vAlign: 0 } },
            { ranges: [`${templateSheetId}!A12:A15`], style: { font: { bold: true }, hAlign: 0, vAlign: 0 } },
            { ranges: [`${templateSheetId}!B12:F15`], style: { hAlign: 0, vAlign: 0 } },
            { ranges: [`${templateSheetId}!A8:F8`], style: { font: { bold: true }, hAlign: 1 } },
            { ranges: [`${templateSheetId}!A18:F18`], style: { font: { bold: true } } },
            { ranges: [`${templateSheetId}!A16:F18`], style: { borderType: "FULL_BORDER", borderColor: "#000000" } },
            { ranges: [`${templateSheetId}!A16:F16`], style: { font: { bold: true }, backColor: "#D9D9D9", hAlign: 1 } },
        ]
    };
    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/styles_batch_update`, {
        method: 'PUT', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(stylePayload)
    });

    // D. Logo
    try {
        // 1. TẠO FILE SPREADSHEET MỚI
        const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ title })
        });
        const createData = await createRes.json();
        if (createData.code !== 0) throw new Error(`Lỗi tạo file: ${createData.msg}`);
        
        const ssToken = createData.data.spreadsheet.spreadsheet_token;
        const ssUrl = createData.data.spreadsheet.url;

        // 2. LẤY ID CỦA SHEET MẶC ĐỊNH
        const queryRes1 = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, {
            method: 'GET', headers: { 'Authorization': `Bearer ${tenantToken}` }
        });
        const queryData1 = await queryRes1.json();
        const templateSheetId = queryData1.data.sheets[0].sheet_id;

        // 3. APPLY TEMPLATE (Values, Merge, Style)
        // A. Values
        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
            method: 'PUT',
            headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ valueRange: { range: `${templateSheetId}!A1:F19`, values: INVOICE_TEMPLATE } })
        });

        // B. Merge
        const mergeRanges = [
            `${templateSheetId}!A1:C4`, `${templateSheetId}!A5:F5`, `${templateSheetId}!A6:F6`,
            `${templateSheetId}!A7:F7`, `${templateSheetId}!A8:F8`, `${templateSheetId}!A18:D18`,
            `${templateSheetId}!A19:F19`, `${templateSheetId}!B12:F12`, `${templateSheetId}!B13:F13`,
            `${templateSheetId}!B14:F14`, `${templateSheetId}!B15:F15`
        ];
        for (const mRange of mergeRanges) {
            await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/merge_cells`, {
                method: 'POST', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ range: mRange, mergeType: "MERGE_ALL" })
            });
        }

        // C. Style
        const stylePayload = {
            data: [
                { ranges: [`${templateSheetId}!A1:C4`], style: { hAlign: 1, vAlign: 1 } },
                { ranges: [`${templateSheetId}!A5:F5`], style: { font: { bold: true }, hAlign: 0, vAlign: 0 } },
                { ranges: [`${templateSheetId}!A6:F7`], style: { hAlign: 0, vAlign: 0 } },
                { ranges: [`${templateSheetId}!A12:A15`], style: { font: { bold: true }, hAlign: 0, vAlign: 0 } },
                { ranges: [`${templateSheetId}!B12:F15`], style: { hAlign: 0, vAlign: 0 } },
                { ranges: [`${templateSheetId}!A8:F8`], style: { font: { bold: true }, hAlign: 1 } },
                { ranges: [`${templateSheetId}!A18:F18`], style: { font: { bold: true } } },
                { ranges: [`${templateSheetId}!A16:F18`], style: { borderType: "FULL_BORDER", borderColor: "#000000" } },
                { ranges: [`${templateSheetId}!A16:F16`], style: { font: { bold: true }, backColor: "#D9D9D9", hAlign: 1 } },
            ]
        };
        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/styles_batch_update`, {
            method: 'PUT', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(stylePayload)
        });

        // 4. NHÂN BẢN TEMPLATE (Đã thêm cơ chế Batching & Bắt lỗi)
        debugLogs.push(`📝 Đang nhân bản thành ${rows.length} Tabs...`);
        const sheetTitles = rows.map((row, index) => sanitizeSheetName(row.waybillNumber, index));
        
        // CẮT NHỎ REQUEST RA MỖI LẦN CHỈ COPY 10 SHEETS (Giảm tải cho Lark)
        const COPY_BATCH_SIZE = 10;
        for (let i = 0; i < sheetTitles.length; i += COPY_BATCH_SIZE) {
            const batchTitles = sheetTitles.slice(i, i + COPY_BATCH_SIZE);
            const copyRequests = batchTitles.map(title => ({
                copySheet: { source: { sheetId: templateSheetId }, destination: { title } }
            }));
            
            const copyRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                method: 'POST', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ requests: copyRequests })
            });
            const copyData = await copyRes.json();
            
            if (copyData.code !== 0) {
                console.error("❌ Lỗi Copy Sheet:", copyData);
                debugLogs.push(`⚠️ Lỗi khi copy sheets (từ ${i} đến ${i + batchTitles.length}): ${copyData.msg}`);
            }
        }

        // 5. LẤY DANH SÁCH SHEET_ID MỚI
        const queryRes2 = await fetch(`https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/${ssToken}/sheets/query`, {
            method: 'GET', headers: { 'Authorization': `Bearer ${tenantToken}` }
        });
        const queryData2 = await queryRes2.json();
        const sheetIdMap = {};
        queryData2.data.sheets.forEach(s => { sheetIdMap[s.title] = s.sheet_id; });

        // 6. ĐIỀN DỮ LIỆU
        debugLogs.push(`⚡ Đang điền dữ liệu hàng loạt...`);
        let allValueRanges = [];
        let missingSheetsCount = 0;

        for (let index = 0; index < rows.length; index++) {
            const row = rows[index];
            const expectedTitle = sheetTitles[index];
            const targetId = sheetIdMap[expectedTitle];
            
            // Xử lý trường hợp sheet không được tạo thành công
            if (!targetId) {
                missingSheetsCount++;
                console.log(`⚠️ Không tìm thấy ID cho sheet: ${expectedTitle}`);
                continue; 
            }

            const valueRanges = row.fields.filter(f => f.val).map(f => ({
                range: `${targetId}!${f.range}`,
                values: [[String(f.val)]]
            }));
            allValueRanges.push(...valueRanges);
        }

        if (missingSheetsCount > 0) {
            debugLogs.push(`🚨 CẢNH BÁO: Rớt ${missingSheetsCount} sheet không thể tạo.`);
        }

        if (allValueRanges.length > 0) {
            const rangeChunks = chunkArray(allValueRanges, 150);
            for (const chunk of rangeChunks) {
                const updateRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values_batch_update`, {
                    method: 'POST', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
                    body: JSON.stringify({ valueRanges: chunk })
                });
                const updateData = await updateRes.json();
                if (updateData.code !== 0) {
                    console.error("❌ Lỗi Update Value:", updateData);
                    debugLogs.push(`⚠️ Lỗi điền dữ liệu: ${updateData.msg}`);
                }
            }
        }

        // 7. DỌN DẸP
        await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
            method: 'POST', headers: { 'Authorization': `Bearer ${tenantToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ requests: [{ deleteSheet: { sheetId: templateSheetId } }] })
        });

        debugLogs.push(`✅ Hoàn tất File: ${title}`);
        return { title, url: ssUrl, rowCount: rows.length - missingSheetsCount };

    } catch (error) {
        console.error("🚨 Fatal Error trong quá trình tạo Batch:", error);
        debugLogs.push(`❌ Lỗi nghiêm trọng: ${error.message}`);
        return { title: `${fileName} (LỖI)`, url: "", rowCount: 0 };
    }
}


// =========================================================================
// ROUTER CHÍNH
// =========================================================================
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
                    debugLogs.push(`🚀 Nhận file: ${file_name}`);

                    const tokenRes = await client.auth.tenantAccessToken.internal({ data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET } });
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

                        const rawDesc = extractAttribute(row, 'item description') || "";
                        const qtyMatch = rawDesc.match(/^(\d+(\.\d+)?)/);
                        const qtyVal = qtyMatch ? qtyMatch[0] : "1";
                        const englishName = cleanProductKey(rawDesc);
                        const vietnameseName = translateProductName(englishName);
                        const ProductName = vietnameseName ? `${englishName}\n(${vietnameseName})` : englishName;
                        const productUnit = getProductUnit(rawDesc);
                        const numericQty = parseFloat(qtyVal) || 1;
                        const totalAmount = (30 * numericQty).toFixed(1);

                        return {
                            waybillNumber: extractAttribute(row, 'waybill number') || `WB_${Math.floor(Math.random() * 1000)}`,
                            fields: [
                                { val: extractAttribute(row, 'shipment date'), range: "F10:F10" },
                                { val: extractAttribute(row, 'recipient name'), range: "B12:B12" },
                                { val: fullAddress, range: "B13:B13" },
                                { val: extractAttribute(row, 'email'), range: "B14:B14" },
                                { val: extractAttribute(row, 'recipient phone'), range: "B15:B15" },
                                { val: ProductName, range: "B17:B17" },
                                { val: productUnit, range: "C17:C17" },
                                { val: qtyVal, range: "E17:E17" },
                                { val: totalAmount, range: "F17:F17" },
                                { val: qtyVal, range: "E18:E18" },
                                { val: totalAmount, range: "F18:F18" }
                            ]
                        };
                    });

                    const rowChunks = chunkArray(rowsData, CHUNK_SIZE);
                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'text',
                            content: JSON.stringify({ text: `⏳ Đã nhận ${rowsData.length} đơn.\n🚀 Đang tiến hành tạo ĐỒNG THỜI ${rowChunks.length} file (mỗi file ${CHUNK_SIZE} sheets)...` })
                        }
                    });
                    const createdSheets = [];

                    // Khởi chạy tạo các Batch
                    for (let batchIndex = 0; batchIndex < rowChunks.length; batchIndex++) {
                        const batchRows = rowChunks[batchIndex];
                        const batchResult = await createSpreadsheetForBatch(tenantToken, `Invoices: ${file_name}`, batchIndex, batchRows, debugLogs);
                        createdSheets.push(batchResult);
                    }

                    await connectDB();
                    await (new CsvVault({ fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData })).save();

                    const actions = createdSheets.map(sheet => ({
                        tag: 'button', text: { tag: 'plain_text', content: `${sheet.title} (${sheet.rowCount})` }, type: 'primary', url: sheet.url
                    }));

                    // const finalLogText = debugLogs.join('\n').substring(0, 3000);

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: 'TẠO HÓA ĐƠN HOÀN TẤT' }, template: "green" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File:** ${file_name}\n📊 **Số Invoice:** ${rowsData.length}` } },
                                    { tag: 'hr' },
                                    { tag: 'action', actions: actions.slice(0, 5) },
                                    // { tag: 'div', text: { tag: 'lark_md', content: `**🖥️ VERCEL DEBUG LOGS:**\n\`\`\`\n${finalLogText}\n\`\`\`` } }
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
                    content: JSON.stringify({ text: `❌ LỖI HỆ THỐNG:\n${error.message}\n${error.stack}` })
                }
            });
        }
    }
    return res.status(200).json({ success: true });
});

module.exports = app;
if (process.env.NODE_ENV !== 'production') app.listen(3000, () => console.log(`🚀 Server running!`));