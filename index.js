require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');

const app = express();
app.use(express.json()); 

// ==========================================
// 1. KẾT NỐI MONGODB (SERVERLESS)
// ==========================================
let isConnected = false;
async function connectDB() {
    if (isConnected) return;
    try {
        await mongoose.connect(process.env.MONGODB_URI);
        isConnected = true;
        console.log('✅ Đã kết nối vào Database: Lark_app');
    } catch (err) {
        console.error('❌ Lỗi kết nối MongoDB:', err);
    }
}

// Định nghĩa Schema
const TaskSchema = new mongoose.Schema({
    title: String,
    project: String,
    status: { type: String, default: 'pending' },
    createdAt: { type: Date, default: Date.now }
});

// ⚠️ ĐIỂM THAY ĐỔI: Ép buộc lưu vào đúng collection "lark"
const Task = mongoose.model('Task', TaskSchema, 'lark');

const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// ==========================================
// 2. XỬ LÝ EVENT TỪ LARK
// ==========================================
app.post('/webhook/event', async (req, res) => {
    const data = req.body || {};

    if (data.type === 'url_verification') {
        return res.status(200).json({ challenge: data.challenge });
    }

    if (data.header && data.header.event_type === 'im.message.receive_v1') {
        const message = data.event.message;
        if (message.message_type !== 'text') return res.status(200).json({ success: true });

        const rawContent = JSON.parse(message.content).text;
        const contentStr = rawContent.toLowerCase();

        await connectDB();

        // TÍNH NĂNG 1: Tạo Task mới
        if (contentStr.startsWith('add task')) {
            const taskTitle = rawContent.replace(/add task/i, '').trim();
            let projectName = taskTitle.toUpperCase().includes('HANDDN') ? 'HANDDN' : 
                              taskTitle.toUpperCase().includes('WILD & KING') ? 'Wild & King' : 'Chung';

            await Task.create({ title: taskTitle, project: projectName });
            
            await client.im.message.create({
                params: { receive_id_type: 'chat_id' },
                data: {
                    receive_id: message.chat_id,
                    msg_type: 'text',
                    content: JSON.stringify({ text: `✅ Đã tạo task mới: "${taskTitle}" (Lưu vào bảng lark)` })
                }
            });
        }

        // TÍNH NĂNG 2: Liệt kê Task
        else if (contentStr.includes('task')) {
            const pendingTasks = await Task.find({ status: 'pending' }).sort({ createdAt: -1 });
            let taskListText = pendingTasks.length > 0 
                ? pendingTasks.map((t, i) => `**${i + 1}. [${t.project}]** ${t.title}`).join('\n')
                : "🎉 Không có task nào đang chờ xử lý!";

            await client.im.message.create({
                params: { receive_id_type: 'chat_id' },
                data: {
                    receive_id: message.chat_id,
                    msg_type: 'interactive',
                    content: JSON.stringify({
                        header: { title: { tag: 'plain_text', content: '📋 Danh sách Task' }, template: "blue" },
                        elements: [
                            { tag: 'div', text: { tag: 'lark_md', content: taskListText } },
                            ...(pendingTasks.length > 0 ? [{
                                tag: 'action', 
                                actions: [{
                                    tag: 'button',
                                    text: { tag: 'plain_text', content: '✅ Hoàn thành tất cả' },
                                    type: 'primary',
                                    value: { action: 'mark_all_done' } 
                                }]
                            }] : [])
                        ]
                    })
                }
            });
        }
    }
    return res.status(200).json({ success: true });
});

app.post('/webhook/card', async (req, res) => {
    const data = req.body || {};
    if (data.type === 'url_verification') return res.status(200).json({ challenge: data.challenge });

    if (data.action?.value?.action === 'mark_all_done') {
        await connectDB();
        await Task.updateMany({ status: 'pending' }, { status: 'done' });
        return res.status(200).json({ toast: { type: 'success', content: '🎉 Tất cả task đã hoàn thành!' } });
    }
    return res.status(200).json({ success: true });
});

module.exports = app;