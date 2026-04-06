require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const lark = require('@larksuiteoapi/node-sdk');

const app = express();
// ⚠️ Đã xóa app.use(express.json()) để thư viện Lark tự động xử lý gói tin (tránh lỗi JSON format)

// ==========================================
// 1. KẾT NỐI MONGODB & TẠO DATABASE MODEL
// ==========================================
mongoose.connect(process.env.MONGODB_URI)
    .then(() => console.log('✅ Đã kết nối MongoDB (Database: lark) thành công!'))
    .catch(err => console.error('❌ Lỗi kết nối MongoDB:', err));

// Định nghĩa cấu trúc bảng dữ liệu
const TaskSchema = new mongoose.Schema({
    title: String,
    project: String,
    status: { type: String, default: 'pending' } 
});

// Ép lưu vào đúng thư mục lark_app
const Task = mongoose.model('Task', TaskSchema, 'lark_app');

// ==========================================
// 2. KHỞI TẠO LARK CLIENT & XỬ LÝ SỰ KIỆN
// ==========================================
const client = new lark.Client({
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
});

// Hàm 2.1: Lắng nghe tin nhắn chat
const eventDispatcher = new lark.EventDispatcher({}).register({
    'im.message.receive_v1': async (data) => {
        try {
            const message = data.message;
            if (message.message_type !== 'text') return { success: true };

            const contentStr = JSON.parse(message.content).text.toLowerCase();

            // Kiểm tra nếu tin nhắn có chứa chữ "task"
            if (contentStr.includes('task')) {
                // Lấy danh sách việc chưa làm từ MongoDB
                const pendingTasks = await Task.find({ status: 'pending' });

                let taskListText = "Tuyệt vời, không có task nào đang tồn đọng!";
                let hasTask = false;

                if (pendingTasks.length > 0) {
                    hasTask = true;
                    taskListText = pendingTasks.map((t, i) => `**${i + 1}. [${t.project || 'Chung'}]** ${t.title}`).join('\n');
                }

                // Cấu hình các khối (elements) cho thẻ Card
                const cardElements = [
                    { tag: 'div', text: { tag: 'lark_md', content: taskListText } }
                ];

                // Chỉ thêm nút bấm "Xong hết rồi" nếu có task
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

                // Gửi Thẻ tương tác (Interactive Card) vào nhóm Lark
                await client.im.message.create({
                    params: { receive_id_type: 'chat_id' },
                    data: {
                        receive_id: message.chat_id,
                        msg_type: 'interactive',
                        content: JSON.stringify({
                            header: { 
                                title: { tag: 'plain_text', content: '📋 Cập nhật công việc' },
                                template: "blue"
                            },
                            elements: cardElements
                        })
                    }
                });
            }
        } catch (error) {
            console.error("Lỗi khi xử lý tin nhắn:", error);
        }
        return { success: true }; // Phải trả về success để Lark biết đã nhận được
    }
});

// Hàm 2.2: Xử lý khi người dùng bấm nút trên Thẻ
const cardActionHandler = new lark.CardActionHandler({}).register('mark_all_done', async (data) => {
    try {
        // Chuyển toàn bộ task đang "pending" thành "done" trong database
        await Task.updateMany({ status: 'pending' }, { status: 'done' });
        console.log("✅ Đã cập nhật trạng thái các task thành 'done'.");

        // Gửi popup thành công trên màn hình người bấm
        return {
            toast: {
                type: 'success',
                content: '🎉 Quá đỉnh! Tất cả công việc đã được giải quyết.'
            }
        };
    } catch (error) {
        console.error("Lỗi khi cập nhật task:", error);
    }
});

// ==========================================
// 3. MỞ CỔNG NHẬN DỮ LIỆU TỪ LARK (ROUTER)
// ==========================================
app.post('/webhook/event', lark.adaptExpress(eventDispatcher));
app.post('/webhook/card', lark.adaptExpress(cardActionHandler));

// ==========================================
// 4. XUẤT APP CHO VERCEL
// ==========================================
module.exports = app;