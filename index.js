require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');

const app = express();
// 1. Phải có dòng này để Vercel đọc được dữ liệu JSON
app.use(express.json()); 

// ==========================================
// KẾT NỐI MONGODB
// ==========================================
mongoose.connect(process.env.MONGODB_URI)
    .then(() => console.log('✅ Đã kết nối MongoDB thành công!'))
    .catch(err => console.error('❌ Lỗi kết nối MongoDB:', err));

const TaskSchema = new mongoose.Schema({
    title: String,
    project: String,
    status: { type: String, default: 'pending' } 
});
const Task = mongoose.model('Task', TaskSchema, 'lark_app');

// ==========================================
// KHỞI TẠO LARK CLIENT
// ==========================================
const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// ==========================================
// API 1: LẮNG NGHE TIN NHẮN (THAY THẾ ADAPT EXPRESS)
// ==========================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    // BƯỚC QUAN TRỌNG NHẤT: Trả lời mã Challenge của Lark để qua ải "JSON format"
    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    // Xử lý khi có tin nhắn thật gửi đến
    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;
        if (message.message_type !== 'text') return res.status(200).json({ success: true });

        const contentStr = JSON.parse(message.content).text.toLowerCase();

        // Nếu chat chữ "task"
        if (contentStr.includes('task')) {
            const pendingTasks = await Task.find({ status: 'pending' });
            let taskListText = "Tuyệt vời, không có task nào đang tồn đọng!";
            let hasTask = false;

            if (pendingTasks.length > 0) {
                hasTask = true;
                taskListText = pendingTasks.map((t, i) => `**${i + 1}. [${t.project || 'Chung'}]** ${t.title}`).join('\n');
            }

            const cardElements = [
                { tag: 'div', text: { tag: 'lark_md', content: taskListText } }
            ];

            if (hasTask) {
                cardElements.push({
                    tag: 'action', 
                    actions: [{
                        tag: 'button',
                        text: { tag: 'plain_text', content: '✅ Xong hết rồi (Done)' },
                        type: 'primary',
                        value: { action: 'mark_all_done' } 
                    }]
                });
            }

            await client.im.message.create({
                params: { receive_id_type: 'chat_id' },
                data: {
                    receive_id: message.chat_id,
                    msg_type: 'interactive',
                    content: JSON.stringify({
                        header: { title: { tag: 'plain_text', content: '📋 Cập nhật công việc' }, template: "blue" },
                        elements: cardElements
                    })
                }
            });
        }
    }
    
    // Luôn trả về 200 OK để Lark không báo lỗi
    return res.status(200).json({ success: true });
});

// ==========================================
// API 2: XỬ LÝ KHI BẤM NÚT TRÊN CARD
// ==========================================
app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};

    // Cần Challenge cả ở Card để lưu link thành công
    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    // Xử lý logic bấm nút hoàn thành
    if (data.action && data.action.value && data.action.value.action === 'mark_all_done') {
        await Task.updateMany({ status: 'pending' }, { status: 'done' });
        console.log("✅ Đã hoàn thành task.");

        // Phản hồi Toast Popup
        return res.status(200).json({
            toast: {
                type: 'success',
                content: '🎉 Quá đỉnh! Tất cả công việc đã được giải quyết.'
            }
        });
    }
    
    return res.status(200).json({ success: true });
});

// Xuất cho Vercel
module.exports = app;