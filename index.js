require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');
const Papa = require('papaparse');

const app = express();
app.use(express.json());

// ==========================================
// 🔑 TOKEN USER (BẮT BUỘC ĐỂ TẠO SPREADSHEET)
// Lưu ý: Nếu báo lỗi "Access denied", hãy vào API Explorer lấy Token mới gắn vào đây
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

// Cấu trúc Database lưu Csv
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
// 🟢 WEBHOOK 1: NHẬN SỰ KIỆN (TẢI FILE, ĐỌC DỮ LIỆU, TẠO SHEET)
// =====================================================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    // 1. Xác thực bảo mật Lark
    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    // 2. Chặn Lark gửi lặp sự kiện (Anti-Spam)
    const eventId = data.header && data.header.event_id;
    if (eventId) {
        if (processedEvents.has(eventId)) {
            console.log(`⏩ Đã bỏ qua sự kiện lặp lại: ${eventId}`);
            return res.status(200).json({ success: true });
        }
        processedEvents.add(eventId);
        setTimeout(() => processedEvents.delete(eventId), 10 * 60 * 1000); // Tự xóa ID sau 10p
    }

    // 3. Xử lý logic chính
    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;

        try {
            if (message.message_type === 'file') {
                const { file_name, file_key } = JSON.parse(message.content);

                if (file_name.toLowerCase().endsWith('.csv')) {
                    console.log(`\n========================================`);
                    console.log(`📂 BẮT ĐẦU XỬ LÝ FILE: ${file_name}`);
                    console.log(`========================================`);

                    // --- BƯỚC 1: LẤY TOKEN & TẢI FILE TRỰC TIẾP ---
                    const tokenRes = await client.auth.tenantAccessToken.internal({
                        data: { app_id: process.env.LARK_APP_ID, app_secret: process.env.LARK_APP_SECRET }
                    });
                    const tenantToken = tokenRes.tenant_access_token;
                    
                    const fileUrl = `https://open.larksuite.com/open-apis/im/v1/messages/${message.message_id}/resources/${file_key}?type=file`;
                    const fetchRes = await fetch(fileUrl, {
                        headers: { 'Authorization': `Bearer ${tenantToken}` }
                    });

                    if (!fetchRes.ok) throw new Error(`Lỗi kéo file HTTP: ${fetchRes.statusText}`);

                    const fileBuffer = Buffer.from(await fetchRes.arrayBuffer());
                    let csvString = fileBuffer.toString('utf-8').replace(/^\uFEFF/, ''); // Xóa BOM Excel
                    
                    if (!csvString.trim()) throw new Error("File rỗng.");

                    // --- BƯỚC 2: BÓC TÁCH DỮ LIỆU & IN LOG ĐỘNG ---
                    const parsed = Papa.parse(csvString, {
                        header: true, skipEmptyLines: 'greedy', dynamicTyping: true, transformHeader: (h) => h.trim()
                    });
                    const rowsData = parsed.data;

                    console.log(`\n--- KIỂM TRA DỮ LIỆU TRONG FILE ---`);
                    rowsData.forEach((row, index) => { 
                        console.log(`[Dòng ${index + 1}]:`); 
                        for (const [columnName, value] of Object.entries(row)) {
                            console.log(`   🔸 ${columnName}: ${value}`);
                        }
                    });
                    console.log(`-----------------------------------\n`);

                    // --- BƯỚC 3: GỘP NHÓM THEO "SHIPMENT DATE" ---
                    const groupedByDate = {};
                    rowsData.forEach(row => {
                        const dateKey = Object.keys(row).find(k => k.toLowerCase().includes('shipment date'));
                        let dateVal = dateKey && row[dateKey] ? String(row[dateKey]).trim() : 'Unknown_Date';
                        
                        // Làm sạch tên Tab (không được chứa các ký tự đặc biệt)
                        dateVal = dateVal.replace(/[\\/?*[\]:]/g, '-').substring(0, 31);
                        
                        if (!groupedByDate[dateVal]) groupedByDate[dateVal] = [];
                        groupedByDate[dateVal].push(row);
                    });

                    // --- BƯỚC 4: TẠO SPREADSHEET TRÊN LARK ---
                    console.log(`📝 Đang tạo Lark Spreadsheet...`);
                    const createRes = await fetch('https://open.larksuite.com/open-apis/sheets/v3/spreadsheets', {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ title: `Báo Cáo Shipment: ${file_name}` })
                    });
                    const createData = await createRes.json();
                    
                    if (createData.code !== 0) throw new Error(`Lỗi tạo Spreadsheet: ${createData.msg}`);
                    
                    const ssToken = createData.data.spreadsheet.spreadsheet_token;
                    const ssUrl = createData.data.spreadsheet.url;

                    // --- BƯỚC 5: TẠO CÁC TAB (SHEET) TƯƠNG ỨNG ---
                    const sheetNames = Object.keys(groupedByDate);
                    console.log(`📑 Đang tạo ${sheetNames.length} Tab theo ngày...`);
                    const tabRequests = sheetNames.map(name => ({ addSheet: { properties: { title: name } } }));
                    
                    await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/sheets_batch_update`, {
                        method: 'POST',
                        headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ requests: tabRequests })
                    });

                    // --- BƯỚC 6: BƠM DỮ LIỆU VÀO TỪNG TAB ---
                    console.log(`🚀 Bắt đầu bơm dữ liệu vào các Sheet...`);
                    for (const sheetName of sheetNames) {
                        const rows = groupedByDate[sheetName];
                        if (rows.length === 0) continue;
                        
                        const headers = Object.keys(rows[0]);
                        const values = [headers]; // Tiêu đề là dòng đầu tiên
                        
                        // Xử lý dữ liệu Null/Undefined thành chuỗi rỗng
                        rows.forEach(r => {
                            values.push(headers.map(h => r[h] !== null && r[h] !== undefined ? String(r[h]) : ""));
                        });

                        const endColLetter = getColLetter(headers.length - 1);
                        const endRow = values.length;
                        const range = `${sheetName}!A1:${endColLetter}${endRow}`;

                        const writeRes = await fetch(`https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/${ssToken}/values`, {
                            method: 'PUT',
                            headers: { 'Authorization': `Bearer ${USER_TOKEN}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ valueRange: { range: range, values: values } })
                        });
                        
                        const writeResult = await writeRes.json();
                        if (writeResult.code === 0) {
                            console.log(`✅ Bơm thành công: [${sheetName}]`);
                        } else {
                            console.error(`❌ Lỗi bơm dữ liệu [${sheetName}]: ${writeResult.msg}`);
                        }
                    }

                    // --- BƯỚC 7: LƯU MONGODB & GỬI TIN NHẮN PHẢN HỒI ---
                    await connectDB();
                    const newFileEntry = new CsvVault({
                        fileName: file_name, fileContentRaw: csvString, totalRows: rowsData.length, parsedData: rowsData
                    });
                    const savedDoc = await newFileEntry.save();
                    console.log(`✅ Đã lưu Backup vào MongoDB`);

                    await client.im.message.reply({
                        path: { message_id: message.message_id },
                        data: {
                            msg_type: 'interactive',
                            content: JSON.stringify({
                                header: { title: { tag: 'plain_text', content: '✅ XỬ LÝ HOÀN TẤT' }, template: "blue" },
                                elements: [
                                    { tag: 'div', text: { tag: 'lark_md', content: `📝 **File gốc:** ${file_name}\n📊 **Số Shipment Date:** ${sheetNames.length}\n🗂️ **Tổng dòng dữ liệu:** ${rowsData.length}` } },
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
                    console.log(`🎉 HOÀN THÀNH QUY TRÌNH!\n`);
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
// 🔵 WEBHOOK 2: XỬ LÝ NÚT BẤM (XÓA DATABASE)
// =====================================================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    
    // Bỏ qua kiểm tra URL cho Card Action
    if (data.type === 'url_verification') return res.status(200).json({ challenge: data.challenge });

    if (data.action && data.action.value && data.action.value.action === 'delete_file') {
        const { docId } = data.action.value;
        try {
            await connectDB();
            await CsvVault.findByIdAndDelete(docId);
            return res.status(200).json({ toast: { type: 'success', content: 'Đã dọn dẹp dữ liệu gốc khỏi MongoDB!' } });
        } catch (err) {
            return res.status(200).json({ toast: { type: 'error', content: 'Lỗi khi xóa bản ghi.' } });
        }
    }
    return res.status(200).json({ success: true });
});

// =====================================================================
// KHỞI ĐỘNG SERVER (Dành cho Local hoặc môi trường VM)
// =====================================================================
module.exports = app;
if (process.env.NODE_ENV !== 'production') {
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
        console.log(`\n================================`);
        console.log(`🚀 Server Bot đang chạy tại Port: ${PORT}`);
        console.log(`================================\n`);
    });
}