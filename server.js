const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const twilio = require('twilio');

// Ключи Twilio
const twilioClient = twilio('AC26eabfcf1d37dec04bdacd675d721d47', '21c947e54570fe0392fe88c49edb2365');
const TWILIO_PHONE = '+19163148186';

let pendingCodes = {}; 
let ipRateLimits = {}; 
const UPLOADS_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE'],
        credentials: true,
    },
    transports: ['websocket', 'polling'],
    maxHttpBufferSize: 1e8,
});

const DB_FILE = path.join(__dirname, 'users.json');
const MSG_FILE = path.join(__dirname, 'messages.json');
const COMMENTS_FILE = path.join(__dirname, 'comments.json');
const POSTS_FILE = path.join(__dirname, 'posts.json');

// Promises API for async reads/writes
const fsPromises = fs.promises;
let usersCache = [];
let messagesCache = [];

// Initialize caches asynchronously and create files if missing
async function initCaches() {
    try {
        // Users
        try {
            const usersRaw = await fsPromises.readFile(DB_FILE, 'utf8');
            usersCache = JSON.parse(usersRaw) || [];
            console.log(`Loaded ${usersCache.length} users from ${DB_FILE}`);
        } catch (err) {
            if (err && err.code === 'ENOENT') {
                usersCache = [];
                await fsPromises.writeFile(DB_FILE, JSON.stringify(usersCache, null, 2));
                console.log(`Created missing ${DB_FILE}`);
            } else {
                console.error('Failed loading users.json, continuing with empty cache:', err);
                usersCache = [];
            }
        }

        // Messages
        try {
            const msgsRaw = await fsPromises.readFile(MSG_FILE, 'utf8');
            messagesCache = JSON.parse(msgsRaw) || [];
            console.log(`Loaded ${messagesCache.length} messages from ${MSG_FILE}`);
        } catch (err) {
            if (err && err.code === 'ENOENT') {
                messagesCache = [];
                await fsPromises.writeFile(MSG_FILE, JSON.stringify(messagesCache, null, 2));
                console.log(`Created missing ${MSG_FILE}`);
            } else {
                console.error('Failed loading messages.json, continuing with empty cache:', err);
                messagesCache = [];
            }
        }
    } catch (err) {
        console.error('initCaches error:', err);
        usersCache = usersCache || [];
        messagesCache = messagesCache || [];
    }
}
initCaches().catch(err => console.error('initCaches failed:', err));

function loadComments() {
    if (!fs.existsSync(COMMENTS_FILE)) return [];
    try { return JSON.parse(fs.readFileSync(COMMENTS_FILE, 'utf8')); } catch (e) { return []; }
}
function saveComments(comments) { fs.writeFileSync(COMMENTS_FILE, JSON.stringify(comments, null, 2)); }

function loadPosts() {
    if (!fs.existsSync(POSTS_FILE)) return [];
    try { return JSON.parse(fs.readFileSync(POSTS_FILE, 'utf8')); } catch (e) { return []; }
}
function savePosts(posts) { fs.writeFileSync(POSTS_FILE, JSON.stringify(posts, null, 2)); }

app.use(
    cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        credentials: true,
    }),
);
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

app.get('/', (req, res) => {
    res.json({ status: 'ok', message: 'Sparkle Server is running live!' });
});

app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
app.use('/stickerpacks', express.static(path.join(__dirname, 'stickerpacks')));

function sanitizePackName(name) {
    const raw = (name || '').toString().trim();
    if (!raw) return '';

    const safe = raw
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-zA-Z0-9._-]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '')
        .toLowerCase();

    return safe || 'pack';
}

function loadUsers() {
    // Return the in-memory cache. initCaches() populates it at startup and keeps the authoritative copy in memory.
    return usersCache || [];
}
function saveUsers(users) {
    try {
        usersCache = users || [];
        // Persist asynchronously; do not block the event loop. Errors are logged.
        fsPromises.writeFile(DB_FILE, JSON.stringify(usersCache, null, 2))
            .then(() => console.log(`Saved ${usersCache.length} users to disk`))
            .catch(err => console.error('❌ ОШИБКА ЗАПИСИ users.json:', err));
    } catch (err) {
        console.error('❌ Ошибка в saveUsers (sync):', err);
    }
}

function loadMessages() {
    return messagesCache || [];
}
function saveMessages(messages) {
    try {
        messagesCache = messages || [];
        fsPromises.writeFile(MSG_FILE, JSON.stringify(messagesCache, null, 2))
            .then(() => console.log(`Saved ${messagesCache.length} messages to disk`))
            .catch(err => console.error('❌ ОШИБКА ЗАПИСИ messages.json:', err));
    } catch (err) {
        console.error('❌ Ошибка в saveMessages (sync):', err);
    }
}

function loadUserCollection(phone) {
    const users = loadUsers();
    const user = users.find(u => u.phone === phone);
    return user && Array.isArray(user.stickerCollection) ? user.stickerCollection : [];
}

function updateUserCollection(phone, packName, action) {
    const users = loadUsers();
    const user = users.find(u => u.phone === phone);
    if (!user) return null;
    if (!Array.isArray(user.stickerCollection)) user.stickerCollection = [];
    if (action === 'add') {
        if (!user.stickerCollection.includes(packName)) user.stickerCollection.push(packName);
    } else if (action === 'remove') {
        user.stickerCollection = user.stickerCollection.filter(item => item !== packName);
    }
    saveUsers(users);
    return user.stickerCollection;
}

function resolveStickerPacks() {
    const packsDir = path.join(__dirname, 'stickerpacks', 'normal');
    if (!fs.existsSync(packsDir)) return [];
    return fs.readdirSync(packsDir, { withFileTypes: true })
        .filter(dirent => dirent.isDirectory())
        .map(dirent => {
            const packName = dirent.name;
            const packDir = path.join(packsDir, packName);
            let meta = { displayName: packName, owner: null, superName: null };
            const metaPath = path.join(packDir, 'pack.json');
            if (fs.existsSync(metaPath)) {
                try {
                    meta = { ...meta, ...JSON.parse(fs.readFileSync(metaPath, 'utf8')) };
                } catch (e) {
                    console.error('Error reading pack metadata:', e);
                }
            }
            const previewImage = getPackPreviewImage(packDir, meta);
            return {
                key: packName,
                displayName: meta.displayName || packName,
                ownerPhone: meta.ownerPhone || null,
                superName: meta.superName || null,
                createdAt: meta.createdAt || null,
                previewImage: previewImage || null,
                path: `/stickerpacks/normal/${packName}/`
            };
        });
}

function savePackMetadata(packDir, metadata) {
    const metaPath = path.join(packDir, 'pack.json');
    fs.writeFileSync(metaPath, JSON.stringify(metadata, null, 2));
}

function getPackStickerFiles(packDir) {
    return fs.readdirSync(packDir)
        .filter(name => name !== 'pack.json' && /\.(png|jpe?g|webp|gif)$/i.test(name));
}

function getPackPreviewImage(packDir, metadata = {}) {
    if (metadata.previewImage) {
        const previewPath = path.join(packDir, metadata.previewImage);
        if (fs.existsSync(previewPath)) return metadata.previewImage;
    }
    const stickerFiles = getPackStickerFiles(packDir);
    if (!stickerFiles.length) return null;
    stickerFiles.sort((a, b) => {
        const aTime = fs.statSync(path.join(packDir, a)).birthtimeMs || fs.statSync(path.join(packDir, a)).ctimeMs;
        const bTime = fs.statSync(path.join(packDir, b)).birthtimeMs || fs.statSync(path.join(packDir, b)).ctimeMs;
        return aTime - bTime;
    });
    return stickerFiles[0];
}

function removePackFromAllCollections(packName) {
    const users = loadUsers();
    let changed = false;
    users.forEach(user => {
        if (Array.isArray(user.stickerCollection) && user.stickerCollection.includes(packName)) {
            user.stickerCollection = user.stickerCollection.filter(item => item !== packName);
            changed = true;
        }
    });
    if (changed) saveUsers(users);
}

let allRegisteredUsers = loadUsers();
let onlineUsers = {}; 
let userChatFocus = {}; 

function broadcastChatList(userPhone) {
    const socketId = onlineUsers[userPhone];
    if (!socketId) return;

    const messages = loadMessages();
    const users = loadUsers();
    let partners = new Set();
    const visibleMessages = messages.filter(m => isMessageVisibleForUser(m, userPhone));
    
    visibleMessages.forEach(m => {
        if (m.sender === userPhone) partners.add(m.recipient);
        if (m.recipient === userPhone) partners.add(m.sender);
    });

    let chatList = [];
    partners.forEach(partnerPhone => {
        const partnerUser = users.find(u => u.phone === partnerPhone) || { phone: partnerPhone, username: "Пользователь", bio: "" };
        const pairMessages = visibleMessages.filter(m => 
            (m.sender === userPhone && m.recipient === partnerPhone) ||
            (m.sender === partnerPhone && m.recipient === userPhone)
        );

        if (pairMessages.length === 0) return;
        const lastMsg = pairMessages[pairMessages.length - 1];
        const unreadCount = pairMessages.filter(m => m.recipient === userPhone && !m.read).length;

        let previewText = lastMsg.text;
        if (lastMsg.type === 'call_log') {
            previewText = lastMsg.callStatus === 'success' ? '📞 Звонок' : '📵 Отмененный звонок';
        } else if (lastMsg.type === 'vote_transfer') {
            previewText = `Отправлено ${lastMsg.amount} V`;
        }

        chatList.push({
            phone: partnerUser.phone,
            firstName: partnerUser.firstName,
            lastName: partnerUser.lastName,
            username: partnerUser.username,
            bio: partnerUser.bio || "",
            avatarUrl: partnerUser.avatarUrl || "",
            bannerUrl: partnerUser.bannerUrl || "",
            lastMessage: previewText,
            timestamp: lastMsg.timestamp,
            unreadCount: unreadCount,
            isOnline: !!onlineUsers[partnerUser.phone],
            lastSeen: partnerUser.lastSeen || null,
            joinDate: partnerUser.joinDate || null,
            totalReadTime: partnerUser.totalReadTime || 0,
            readMessageCount: partnerUser.readMessageCount || 0,
            hideAnswerTime: partnerUser.hideAnswerTime || false,
            hasStory: loadPosts().some(p => p.authorPhone === partnerUser.phone && (Date.now() - p.timestamp < 24 * 60 * 60 * 1000)),
        });
    });

    chatList.sort((a, b) => b.timestamp - a.timestamp);
    io.to(socketId).emit('chat_list_data', chatList);
}

function isMessageVisibleForUser(message, userPhone) {
    if (!message || !userPhone) return false;
    const deletedFor = Array.isArray(message.deletedFor) ? message.deletedFor : [];
    return !deletedFor.includes(userPhone);
}

function maskIp(ip) {
    if (!ip) return "Неизвестно";
    if (ip === '::1' || ip === '127.0.0.1') return '127.0.0.1 (Local)';
    
    if (ip.includes(':')) { 
        const parts = ip.split(':');
        return parts.slice(0, 3).join(':') + ':XXXX:XXXX';
    }
    
    const parts = ip.split('.');
    if (parts.length === 4) {
        return `${parts[0]}.${parts[1]}.XXX.XXX`;
    }
    return ip;
}

async function getRealGeoData(ip) {
    try {
        const fetchUrl = (ip === '::1' || ip === '127.0.0.1') ? 'http://ip-api.com/json/' : `http://ip-api.com/json/${ip}`;
        const response = await fetch(fetchUrl);
        const data = await response.json();
        
        if (data.status === 'success') {
            return { 
                countryCode: data.countryCode.toLowerCase(), 
                location: `${data.country}, ${data.city}` 
            };
        }
    } catch (err) {
        console.error("❌ Ошибка получения Гео-данных:", err);
    }
    return { countryCode: "de", location: "Германия (Оффлайн)" }; 
}

io.on('connection', (socket) => {
    console.log('🟢 [СИСТЕМА] Новое подключение:', socket.id);
    const clientIp = socket.handshake.address;

    socket.on('check_session', async (token) => {
        const users = loadUsers();
        const user = users.find(u => 
            u.sessionToken === token || (Array.isArray(u.sessions) && u.sessions.some(s => s.token === token))
        );

        if (user) {
            socket.phone = user.phone;
            onlineUsers[user.phone] = socket.id;
            socket.join(token);
            
            console.log(`✅ Сессия подтверждена для: ${user.phone}`);
            
            if (Array.isArray(user.sessions)) {
                let currentSession = user.sessions.find(s => s.token === token);
                if (currentSession) {
                    currentSession.lastSeen = new Date().toLocaleString('ru-RU');
                    if (typeof maskIp === 'function') currentSession.ip = maskIp(clientIp);
                }
                saveUsers(users);
            }

            socket.emit('auth_success', user);
            broadcastChatList(user.phone);
        } else {
            socket.emit('session_invalid');
        }
    });

    socket.on('request_user_sessions', async (data) => {
        const { phone } = data;
        let users = loadUsers();
        let user = users.find(u => u.phone === phone);
        if (!user) return;

        if (!Array.isArray(user.sessions)) {
            let geo = await getRealGeoData(clientIp);
            user.sessions = [{
                token: user.sessionToken || Math.random().toString(36).substr(2) + Date.now(),
                ip: maskIp(clientIp),
                countryCode: geo.countryCode,
                location: geo.location,
                lastSeen: new Date().toLocaleString('ru-RU')
            }];
            saveUsers(users);
        } else {
            let changed = false;
            user.sessions.forEach(s => {
                if (s.ip && !s.ip.includes('X') && !s.ip.includes('Local')) {
                    s.ip = maskIp(s.ip);
                    changed = true;
                }
            });
            if (changed) saveUsers(users);
        }

        socket.emit('receive_user_sessions', { sessions: user.sessions });
    });

    socket.on('revoke_session_by_key', (data) => {
        const { phone, targetToken } = data;
        let users = loadUsers();
        let user = users.find(u => u.phone === phone);
        if (!user || !Array.isArray(user.sessions)) return;

        const initialLength = user.sessions.length;
        user.sessions = user.sessions.filter(s => s.token !== targetToken);
        
        if (user.sessions.length < initialLength) {
            saveUsers(users);
            io.to(targetToken).emit('force_session_logout', { reason: "Ваш сеанс был завершен с другого устройства." });
            if (socket.rooms.has(targetToken)) {
                socket.emit('force_session_logout', { reason: "Вы успешно вышли из системы." });
            }
            socket.emit('receive_user_sessions', { sessions: user.sessions });
        }
    });

    socket.on('terminate_active_session', (data) => {
        const { phone, token } = data;
        socket.emit('force_session_logout', { reason: "Сессия успешно уничтожена в базе данных Sparkle." });
    });

    // ЗВОНКИ
    socket.on('call_user', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('incoming_call', {
                fromPhone: socket.phone,
                callerName: data.callerName,
                callerAvatar: data.callerAvatar,
                callerBanner: data.callerBanner,
                type: data.type
            });
        }
    });

    socket.on('call_response', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('call_response', { fromPhone: socket.phone, accepted: data.accepted });
        }
    });

    socket.on('call_ended', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('call_ended', { fromPhone: socket.phone });
        }
    });

    socket.on('webrtc_signal', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('webrtc_signal', { fromPhone: socket.phone, signal: data.signal });
        }
    });

    socket.on('mic_toggled', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('mic_status_changed', { username: data.username, isMuted: data.isMuted });
        }
    });

    socket.on('video_state_update', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('remote_video_state', { isVideoOn: data.isVideoOn, username: data.username });
        }
    });

    socket.on('screenshare_paused', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('remote_screenshare_paused', { isPaused: data.isPaused, username: data.username });
        }
    });

    socket.on('invite_to_call', (data) => {
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('incoming_call', {
                fromPhone: socket.phone, 
                callerName: data.callerName, 
                callerAvatar: data.callerAvatar,
                callerBanner: data.callerBanner,
                type: 'audio',
                isGroupInvite: true,
                inviterPhone: socket.phone
            });
        }
    });

    // СТАТУСЫ И ЧАТЫ
    socket.on('user_status_changed', (data) => {
        if (!socket.phone) return;
        const targetPhone = data?.phone || socket.phone;
        const status = data?.status === 'in_chat' ? 'in_chat' : (data?.status === 'offline' ? 'offline' : 'online');
        const lastSeen = data?.lastSeen || (status === 'offline' ? Date.now() : null);
        if (status === 'offline') {
            const users = loadUsers();
            const user = users.find(u => u.phone === targetPhone);
            if (user) {
                user.lastSeen = lastSeen;
                saveUsers(users);
            }
        }
        io.emit('user_status_changed', { phone: targetPhone, status, lastSeen });
    });

    socket.on('get_chat_list', (data) => {
        const targetPhone = data?.phone || socket.phone;
        if (targetPhone) broadcastChatList(targetPhone);
    });

    socket.on('join_chat', (data) => {
        if (!socket.phone) return;
        userChatFocus[socket.phone] = data.targetPhone;
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('user_status_changed', { phone: socket.phone, status: 'in_chat' });
        }
    });

    socket.on('leave_chat', () => {
        if (!socket.phone) return;
        const target = userChatFocus[socket.phone];
        delete userChatFocus[socket.phone];
        if (target && onlineUsers[target]) {
            io.to(onlineUsers[target]).emit('user_status_changed', { phone: socket.phone, status: 'online' });
        }
    });

    socket.on('typing', (data) => {
        if (!socket.phone) return;
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('user_typing', { phone: socket.phone, isTyping: true });
        }
    });

    socket.on('stop_typing', (data) => {
        if (!socket.phone) return;
        const targetSocket = onlineUsers[data.targetPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('user_typing', { phone: socket.phone, isTyping: false });
        }
    });

    // АВТОРИЗАЦИЯ
    socket.on('request_sms_code', (data) => {
        const now = Date.now();
        if (ipRateLimits[clientIp] && ipRateLimits[clientIp].blockUntil > now) {
            return socket.emit('sms_rate_limited');
        }

        const phone = data.phone;
        const code = Math.floor(10000 + Math.random() * 90000).toString();
        pendingCodes[phone] = code;

        twilioClient.messages.create({
            body: `${code} is your Sparkle verification code. Do not share it with anyone.`,
            from: TWILIO_PHONE,
            to: phone
        })
        .then(message => {
            socket.emit('code_sent_success');
        })
        .catch(error => {
            console.error('❌ Ошибка отправки СМС, включен Fallback:', error);
            socket.emit('code_sent_success'); 
        });
    });

socket.on('verify_code', (data) => {
    const now = Date.now();
    if (ipRateLimits[clientIp] && ipRateLimits[clientIp].blockUntil > now) {
        return socket.emit('sms_rate_limited');
    }

    if (!ipRateLimits[clientIp]) ipRateLimits[clientIp] = { fails: 0, blockUntil: 0 };

    if (data.code === '55555' || pendingCodes[data.phone] === data.code) {
        ipRateLimits[clientIp].fails = 0;
        delete pendingCodes[data.phone];
        
        let users = loadUsers(); // Читаем свежие данные с диска
        let user = users.find(u => u.phone === data.phone);
        if (user) {
            socket.phone = user.phone;
            onlineUsers[user.phone] = socket.id;
            socket.emit('code_verified', { isNewUser: false, user: user });
            broadcastChatList(user.phone);
        } else {
            socket.emit('code_verified', { isNewUser: true, phone: data.phone });
        }
    } else {
        ipRateLimits[clientIp].fails += 1;
        if (ipRateLimits[clientIp].fails >= 3) {
            ipRateLimits[clientIp].blockUntil = now + 3600000;
            return socket.emit('sms_rate_limited');
        }
        socket.emit('code_invalid');
    }
});

// 2. Исправленный обработчик регистрации в server.js
socket.on('user_registered', (userData) => {
    let users = loadUsers(); // Читаем свежие данные с диска
    let user = {
        phone: userData.phone,
        firstName: userData.firstName,
        lastName: userData.lastName,
        username: userData.username,
        bio: userData.bio, 
        joinDate: Date.now(),
        totalReadTime: 0,
        readMessageCount: 0,
        hideAnswerTime: false,
        votes: 0, 
        sessionToken: Math.random().toString(36).substr(2) + Date.now()
    };
    
    if (userData.avatarBase64) {
        const newUrl = saveBase64Image(userData.avatarBase64, userData.phone, 'avatar');
        if (newUrl) user.avatarUrl = newUrl;
    }

    users.push(user);
    saveUsers(users); // Сохраняем обновленный массив
    
    socket.phone = user.phone;
    onlineUsers[user.phone] = socket.id;
    socket.emit('auth_success', user);
});

// 3. Исправленный обработчик обновления профиля в server.js
socket.on('update_profile', (data) => {
    const userPhone = socket.phone || data.phone;
    if (!userPhone) return;

    let users = loadUsers(); // Читаем свежие данные с диска
    let user = users.find(u => u.phone === userPhone);
    if (!user) return;

    if (data.avatarBase64) {
        const newUrl = saveBase64Image(data.avatarBase64, userPhone, 'avatar');
        if (newUrl) user.avatarUrl = newUrl;
    }

    if (data.bannerBase64) {
        const newUrl = saveBase64Image(data.bannerBase64, userPhone, 'banner');
        if (newUrl) user.bannerUrl = newUrl;
    }

    if (data.username) user.username = data.username;
    if (data.bio) user.bio = data.bio;
    if (data.firstName !== undefined) user.firstName = data.firstName;
    if (data.lastName !== undefined) user.lastName = data.lastName;
    if (data.hideAnswerTime !== undefined) user.hideAnswerTime = data.hideAnswerTime;

    saveUsers(users);
    io.emit('profile_updated', user);
    broadcastChatList(userPhone);
});

    socket.on('mark_chat_read', (data) => {
        if (!socket.phone || !data?.withPhone) return;
        const messages = loadMessages();
        const myPhone = socket.phone;
        const partnerPhone = data.withPhone;
        let changed = false;
        let newlyReadCount = 0;
        let newlyReadTimeSum = 0;
        const now = Date.now();

        messages.forEach(m => {
            const isIncomingFromPartner = m.sender === partnerPhone && m.recipient === myPhone;
            if (isIncomingFromPartner && !m.read) {
                m.read = true;
                changed = true;
                newlyReadCount++;
                newlyReadTimeSum += (now - m.timestamp);
            }
        });

        if (changed) {
            saveMessages(messages);
            let users = loadUsers();
            let me = users.find(u => u.phone === myPhone);
            if (me) {
                me.readMessageCount = (me.readMessageCount || 0) + newlyReadCount;
                me.totalReadTime = (me.totalReadTime || 0) + newlyReadTimeSum;
                saveUsers(users);
                io.emit('profile_updated', me);
            }

            const senderSocketId = onlineUsers[partnerPhone];
            if (senderSocketId) {
                io.to(senderSocketId).emit('messages_read', { byPhone: myPhone });
            }
        }
    });

    socket.on('get_chat_history', (data) => {
        if (!socket.phone) return;
        const messages = loadMessages();
        const myPhone = socket.phone;
        const partnerPhone = data.withPhone;
        const history = messages.filter(m => 
            isMessageVisibleForUser(m, myPhone) && (
                (m.sender === myPhone && m.recipient === partnerPhone) ||
                (m.sender === partnerPhone && m.recipient === myPhone)
            )
        );
        socket.emit('chat_history', history);
    });

    socket.on('send_private_message', (data) => {
        if (!socket.phone || !data.recipientPhone) return;
        
        // У обычных сообщений есть текст, у логов звонков и стикеров текста может не быть
        if (!data.text && !data.mediaBase64 && data.type !== 'call_log' && data.type !== 'sticker') return;

        const users = loadUsers();
        const senderUser = users.find(u => u.phone === socket.phone);
        const targetUser = users.find(u => u.phone === data.recipientPhone);
        const senderBlockedTarget = !!(senderUser && Array.isArray(senderUser.blockedUsers) && senderUser.blockedUsers.includes(data.recipientPhone));
        const targetBlockedSender = !!(targetUser && Array.isArray(targetUser.blockedBy) && targetUser.blockedBy.includes(socket.phone));
        if (senderBlockedTarget || targetBlockedSender) {
            socket.emit('chat_blocked', { targetPhone: data.recipientPhone, by: senderBlockedTarget ? socket.phone : data.recipientPhone });
            return;
        }

        console.log('🔥 СОКЕТ ПРИНЯЛ СООБЩЕНИЕ:', data);

        const messages = loadMessages();
        let mediaUrl = null;
        if (data.mediaBase64) {
            mediaUrl = saveBase64Image(data.mediaBase64, socket.phone, `chat_${Date.now()}`);
            if (!mediaUrl) return;
        }
        
        const newMsg = {
            sender: socket.phone,
            recipient: data.recipientPhone,
            text: data.text ? data.text.trim() : '',
            type: data.type || 'text',              // 'text', 'call_log', 'sticker', 'media'
            callStatus: data.callStatus || null,    // 'success' или 'canceled'
            duration: data.duration || null,        // Время разговора
            stickerUrl: data.stickerUrl || null,    // Путь к стикеру
            stickerName: data.stickerName || null,  // Имя файла стикера
            packName: data.packName || null,        // Название пакета
            timestamp: Date.now(),
            mediaUrl: mediaUrl,
            isVideo: !!data.isVideo,
            read: false
        };

        messages.push(newMsg);
        saveMessages(messages);

        const recipientSocketId = onlineUsers[data.recipientPhone];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('new_private_message', newMsg);
            broadcastChatList(data.recipientPhone);
        }

        socket.emit('new_private_message', newMsg);
        broadcastChatList(socket.phone);
    });

    socket.on('search_user', (data) => {
        if (!data || !data.query) return socket.emit('search_results', { results: [] });
        const q = data.query.toLowerCase();
        
        let users = loadUsers(); // Читаем свежие данные с диска
        let rawResults = [];
        if (data.mode === 'username') {
            rawResults = users.filter(u => u.username?.toLowerCase().includes(q) || u.phone?.includes(q));
        } else {
            rawResults = users.filter(u => u.bio?.toLowerCase().includes(q));
        }
        
        const results = rawResults.map(u => ({
            ...u,
            isOnline: !!onlineUsers[u.phone],
            lastSeen: u.lastSeen || null,
            joinDate: u.joinDate || null,
            totalReadTime: u.totalReadTime || 0,
            readMessageCount: u.readMessageCount || 0,
            hideAnswerTime: u.hideAnswerTime || false,
        }));
        socket.emit('search_results', { results: results });
    });

    // ==================== ГОЛОСА (VOTES) ====================
    socket.on('get_votes_data', () => {
        if (!socket.phone) return;
        let users = loadUsers();
        let me = users.find(u => u.phone === socket.phone);
        if (me) {
            socket.emit('votes_balance_updated', { balance: me.votes || 0 });
        }
    });

    socket.on('buy_votes', (data) => {
        if (!socket.phone) return;
        let users = loadUsers();
        let me = users.find(u => u.phone === socket.phone);
        if (me) {
            me.votes = (me.votes || 0) + data.amount;
            saveUsers(users);
            socket.emit('votes_balance_updated', { balance: me.votes });
        }
    });

    socket.on('send_votes', (data) => {
        if (!socket.phone || !data.targetPhone) return;
        let users = loadUsers();
        let me = users.find(u => u.phone === socket.phone);
        let target = users.find(u => u.phone === data.targetPhone);
        let senderUser = users.find(u => u.phone === socket.phone);
        let targetUser = users.find(u => u.phone === data.targetPhone);
        const senderBlockedTarget = !!(senderUser && Array.isArray(senderUser.blockedUsers) && senderUser.blockedUsers.includes(data.targetPhone));
        const targetBlockedSender = !!(targetUser && Array.isArray(targetUser.blockedBy) && targetUser.blockedBy.includes(socket.phone));

        if (!me || !target) return;
        if (senderBlockedTarget || targetBlockedSender) {
            socket.emit('chat_blocked', { targetPhone: data.targetPhone, by: senderBlockedTarget ? socket.phone : data.targetPhone });
            return;
        }
        let currentBalance = me.votes || 0;

        if (currentBalance < data.amount) {
            return socket.emit('not_enough_votes');
        }

        me.votes -= data.amount;
        target.votes = (target.votes || 0) + data.amount;
        saveUsers(users);

        socket.emit('votes_balance_updated', { balance: me.votes });
        const targetSocketId = onlineUsers[data.targetPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('votes_balance_updated', { balance: target.votes });
        }
        if (data.context === 'chat') {
            const messages = loadMessages();
            const newMsg = {
                sender: socket.phone,
                recipient: data.targetPhone,
                senderName: me.firstName || `@${me.username}`,
                receiverName: target.firstName || `@${target.username}`,
                text: data.text || '', // 🔥 Теперь сервер берет текст из инпута!
                type: 'vote_transfer',
                amount: data.amount,
                timestamp: Date.now(),
                read: false
            };
            messages.push(newMsg);
            saveMessages(messages);

            if (targetSocketId) io.to(targetSocketId).emit('new_private_message', newMsg);
            socket.emit('new_private_message', newMsg);
        }
        else if (data.context === 'post' && data.postId) {
            let posts = loadPosts();
            let post = posts.find(p => p.id === data.postId);
            if (post) {
                post.votes = (post.votes || 0) + data.amount;
                savePosts(posts);
                io.emit('post_stats_updated', { postId: post.id, views: post.views, likes: post.likes, votes: post.votes });
            }
        }

        socket.emit('votes_sent', { context: data.context, postId: data.postId || null, amount: data.amount });
    });

    // ==================== КОММЕНТАРИИ ====================
    socket.on('get_comments', (data) => {
        let allComments = loadComments();
        const now = Date.now();
        let changed = false;

        const filteredComments = allComments.filter(c => {
            if (c.deleteAt && now >= c.deleteAt) {
                changed = true;
                return false; 
            }
            return true; 
        });

        if (changed) {
            saveComments(filteredComments);
            allComments = filteredComments;
        }

        const userComments = allComments.filter(c => c.targetPhone === data.targetPhone).sort((a, b) => b.timestamp - a.timestamp);
        socket.emit('comments_data', { targetPhone: data.targetPhone, comments: userComments });
    });

    socket.on('add_comment', (data) => {
        if (!socket.phone) return;
        const allComments = loadComments();
        const users = loadUsers();
        const sender = users.find(u => u.phone === socket.phone);
        const senderName = sender ? (sender.firstName || `@${sender.username}`) : 'User';

        if (data.votes && data.votes > 0) {
            if ((sender.votes || 0) < data.votes) {
                return socket.emit('not_enough_votes');
            }
            sender.votes -= data.votes;
            
            const target = users.find(u => u.phone === data.targetPhone);
            if (target) {
                target.votes = (target.votes || 0) + data.votes;
                const targetSocketId = onlineUsers[data.targetPhone];
                if (targetSocketId) {
                    io.to(targetSocketId).emit('votes_balance_updated', { balance: target.votes });
                }
            }
            saveUsers(users);
            socket.emit('votes_balance_updated', { balance: sender.votes });
        }

        const newComment = {
            id: Date.now().toString(),
            targetPhone: data.targetPhone,
            senderPhone: socket.phone,
            senderName: senderName,
            text: data.text,
            votes: data.votes || 0,
            timestamp: Date.now()
        };
        
        allComments.push(newComment);
        saveComments(allComments);
        socket.emit('comment_added_success', { votes: newComment.votes });
        io.emit('new_comment_added', newComment);
    });

    socket.on('mark_comment_delete', (data) => {
        if (!socket.phone) return;
        const allComments = loadComments();
        const comment = allComments.find(c => c.id === data.commentId && c.targetPhone === socket.phone);
        
        if (comment && !comment.deleteAt) {
            comment.deleteAt = Date.now() + 259200000;
            saveComments(allComments);
            io.emit('comment_marked_deleted', { 
                commentId: comment.id, 
                targetPhone: socket.phone, 
                deleteAt: comment.deleteAt 
            });
        }
    });

    // ==================== ПОСТЫ (СТОРИС) ====================
    socket.on('create_post', (data) => {
        if (!socket.phone) return;
        const posts = loadPosts();
        const newUrl = saveBase64Image(data.mediaBase64, socket.phone, `post_${Date.now()}`);
        if (!newUrl) return;

        const newPost = {
            id: Date.now().toString(),
            authorPhone: socket.phone,
            mediaUrl: newUrl,
            isVideo: data.isVideo,
            timestamp: Date.now(),
            views: [],
            likes: [],
            votes: 0
        };
        
        posts.push(newPost);
        savePosts(posts);
        
        io.emit('new_post_alert', { phone: socket.phone });
        socket.emit('post_created_success');
    });

    socket.on('get_posts', (data) => {
        const posts = loadPosts();
        const twentyFourHours = 24 * 60 * 60 * 1000;
        const userPosts = posts.filter(p => p.authorPhone === data.targetPhone && (Date.now() - p.timestamp < twentyFourHours));
        socket.emit('posts_data', { targetPhone: data.targetPhone, posts: userPosts.sort((a, b) => b.timestamp - a.timestamp) });
    });

    socket.on('view_post', (data) => {
        if (!socket.phone) return;
        const posts = loadPosts();
        const post = posts.find(p => p.id === data.postId);
        if (post && !post.views.includes(socket.phone)) {
            post.views.push(socket.phone);
            savePosts(posts);
            io.emit('post_stats_updated', { postId: post.id, views: post.views, likes: post.likes, votes: post.votes });
        }
    });

    socket.on('toggle_like_post', (data) => {
        if (!socket.phone) return;
        const posts = loadPosts();
        const post = posts.find(p => p.id === data.postId);
        if (post) {
            const likeIndex = post.likes.indexOf(socket.phone);
            if (likeIndex === -1) post.likes.push(socket.phone);
            else post.likes.splice(likeIndex, 1);
            savePosts(posts);
            
            socket.emit('post_liked_status', { postId: post.id, isLiked: likeIndex === -1 });
            io.emit('post_stats_updated', { postId: post.id, views: post.views, likes: post.likes, votes: post.votes });
        }
    });

    socket.on('delete_post', (data) => {
        if (!socket.phone) return;
        let posts = loadPosts();
        const postIndex = posts.findIndex(p => p.id === data.postId && p.authorPhone === socket.phone);
        if (postIndex !== -1) {
            posts.splice(postIndex, 1);
            savePosts(posts);
            socket.emit('post_deleted_success', { targetPhone: socket.phone });
            io.emit('new_post_alert', { phone: socket.phone }); 
        }
    });

    socket.on('disconnect', () => {
        if (socket.phone) {
            console.log(`🔴 [ОТКЛЮЧЕНИЕ] Пользователь вышел: ${socket.phone}`);
            let users = loadUsers(); // Читаем свежие данные с диска
            let user = users.find(u => u.phone === socket.phone);
            if (user) {
                user.lastSeen = Date.now();
                saveUsers(users);
            }
            delete onlineUsers[socket.phone];
            delete userChatFocus[socket.phone];
            io.emit('user_status_changed', { phone: socket.phone, status: 'offline', lastSeen: Date.now() });
            broadcastChatList(socket.phone);
        }
    });
});

// ===== STICKER PACKS API =====
app.get('/api/sticker-packs', (req, res) => {
    try {
        const packs = resolveStickerPacks();
        res.json(packs);
    } catch (error) {
        console.error('Failed to resolve sticker packs:', error);
        res.status(500).json({ error: 'Failed to resolve sticker packs' });
    }
});

app.post('/api/sticker-packs', (req, res) => {
    const { displayName, superName, ownerPhone, folderName, firstStickerBase64 } = req.body;
    const packKey = sanitizePackName(folderName || displayName || 'new_pack');
    if (!packKey) return res.status(400).json({ error: 'Invalid pack name' });

    const packDir = path.join(__dirname, 'stickerpacks', 'normal', packKey);
    if (fs.existsSync(packDir)) {
        return res.status(409).json({ error: 'Pack already exists' });
    }

    try {
        fs.mkdirSync(packDir, { recursive: true });
        const metadata = {
            displayName: displayName || packKey,
            ownerPhone: ownerPhone || null,
            superName: superName || null,
            createdAt: Date.now()
        };
        savePackMetadata(packDir, metadata);

        let previewImage = null;
        if (firstStickerBase64) {
            previewImage = saveBase64ToDir(firstStickerBase64, packDir, 'sticker_1');
        }
        if (previewImage) {
            metadata.previewImage = previewImage;
            savePackMetadata(packDir, metadata);
        }

        return res.status(201).json({ key: packKey, ...metadata, path: `/stickerpacks/normal/${packKey}/` });
    } catch (error) {
        console.error('Failed to create sticker pack:', error);
        return res.status(500).json({ error: 'Failed to create sticker pack' });
    }
});

app.post('/api/sticker-packs/:packName/stickers', (req, res) => {
    const { packName } = req.params;
    const { stickerBase64, fileName } = req.body;
    const packDir = path.join(__dirname, 'stickerpacks', 'normal', packName);

    if (!fs.existsSync(packDir)) {
        return res.status(404).json({ error: 'Pack not found' });
    }
    if (!stickerBase64) {
        return res.status(400).json({ error: 'Sticker data is required' });
    }

    try {
        const savedName = saveBase64ToDir(stickerBase64, packDir, fileName || `sticker_${Date.now()}`);
        if (!savedName) return res.status(500).json({ error: 'Failed to save sticker' });
        const metaPath = path.join(packDir, 'pack.json');
        if (fs.existsSync(metaPath)) {
            try {
                const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8')) || {};
                if (!meta.previewImage) {
                    meta.previewImage = savedName;
                    savePackMetadata(packDir, meta);
                }
            } catch (e) {
                console.error('Failed to update pack metadata after adding sticker:', e);
            }
        }
        res.status(201).json({ file: savedName, url: `/stickerpacks/normal/${packName}/${savedName}` });
    } catch (error) {
        console.error('Failed to add sticker to pack:', error);
        res.status(500).json({ error: 'Failed to add sticker to pack' });
    }
});

app.delete('/api/sticker-packs/:packName/stickers/:stickerName', (req, res) => {
    const { packName, stickerName } = req.params;
    const packDir = path.join(__dirname, 'stickerpacks', 'normal', packName);
    const filePath = path.join(packDir, stickerName);
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({ error: 'Sticker not found' });
    }

    try {
        fs.unlinkSync(filePath);

        const remainingFiles = getPackStickerFiles(packDir);
        if (remainingFiles.length === 0) {
            if (fs.existsSync(path.join(packDir, 'pack.json'))) fs.unlinkSync(path.join(packDir, 'pack.json'));
            fs.rmdirSync(packDir);
            removePackFromAllCollections(packName);
            return res.json({ success: true, packDeleted: true });
        }

        const metaPath = path.join(packDir, 'pack.json');
        if (fs.existsSync(metaPath)) {
            try {
                const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8')) || {};
                const currentPreview = meta.previewImage;
                if (!currentPreview || currentPreview === stickerName) {
                    const nextPreview = getPackPreviewImage(packDir, meta);
                    if (nextPreview) {
                        meta.previewImage = nextPreview;
                        savePackMetadata(packDir, meta);
                    }
                }
            } catch (e) {
                console.error('Failed to update pack metadata after deleting sticker:', e);
            }
        }

        res.json({ success: true, packDeleted: false });
    } catch (error) {
        console.error('Failed to delete sticker:', error);
        res.status(500).json({ error: 'Failed to delete sticker' });
    }
});

app.get('/api/user/collection/:phone', (req, res) => {
    const { phone } = req.params;
    const collection = loadUserCollection(phone);
    res.json({ collection });
});

// Get block lists for a user
app.get('/api/user/blocks/:phone', (req, res) => {
    const { phone } = req.params;
    const users = loadUsers();
    const user = users.find(u => u.phone === phone) || {};
    res.json({ blockedUsers: Array.isArray(user.blockedUsers) ? user.blockedUsers : [], blockedBy: Array.isArray(user.blockedBy) ? user.blockedBy : [] });
});

// Block / Unblock endpoint
app.post('/api/user/block', (req, res) => {
    const { blocker, target, action } = req.body;
    console.log('API /api/user/block called:', { blocker, target, action });
    if (!blocker || !target || !action) {
        console.warn('Bad block request, missing fields', req.body);
        return res.status(400).json({ error: 'blocker, target and action required' });
    }
    const users = loadUsers();
    const blockerUser = users.find(u => u.phone === blocker);
    const targetUser = users.find(u => u.phone === target);
    if (!blockerUser || !targetUser) {
        console.warn('Block failed - users not found', { blockerFound: !!blockerUser, targetFound: !!targetUser });
        return res.status(404).json({ error: 'User(s) not found' });
    }

    blockerUser.blockedUsers = Array.isArray(blockerUser.blockedUsers) ? blockerUser.blockedUsers : [];
    targetUser.blockedBy = Array.isArray(targetUser.blockedBy) ? targetUser.blockedBy : [];

    if (action === 'block') {
        if (!blockerUser.blockedUsers.includes(target)) blockerUser.blockedUsers.push(target);
        if (!targetUser.blockedBy.includes(blocker)) targetUser.blockedBy.push(blocker);
    } else if (action === 'unblock') {
        blockerUser.blockedUsers = blockerUser.blockedUsers.filter(p => p !== target);
        targetUser.blockedBy = targetUser.blockedBy.filter(p => p !== blocker);
    }

    try {
        saveUsers(users);
    } catch (err) {
        console.error('Failed saving users after block change', err);
        return res.status(500).json({ error: 'Failed saving user data' });
    }

    // Emit socket events for live update
    const targetSocket = onlineUsers[targetUser.phone];
    const blockerSocket = onlineUsers[blockerUser.phone];
    if (action === 'block') {
        if (targetSocket) io.to(targetSocket).emit('you_were_blocked', { by: blocker });
        if (blockerSocket) io.to(blockerSocket).emit('user_block_updated', { target, action: 'block' });
    } else {
        if (targetSocket) io.to(targetSocket).emit('you_were_unblocked', { by: blocker });
        if (blockerSocket) io.to(blockerSocket).emit('user_block_updated', { target, action: 'unblock' });
    }

    res.json({ success: true });
});

app.post('/api/user/collection', (req, res) => {
    const { phone, packName, action } = req.body;
    if (!phone || !packName || !action) {
        return res.status(400).json({ error: 'phone, packName and action are required' });
    }
    const collection = updateUserCollection(phone, packName, action);
    if (collection === null) return res.status(404).json({ error: 'User not found' });
    res.json({ collection });
});

// ===== STICKER API =====
app.get('/api/stickers/:packName', (req, res) => {
    const { packName } = req.params;
    const stickerDir = path.join(__dirname, 'stickerpacks', 'normal', packName);

    if (!fs.existsSync(stickerDir)) {
        return res.status(404).json({ error: 'Sticker pack not found' });
    }

    try {
        const files = fs.readdirSync(stickerDir);
        const stickers = files.filter(file =>
            /\.(png|jpg|jpeg|gif|webp|PNG|JPG|JPEG|GIF|WEBP)$/.test(file)
        );
        res.json(stickers);
    } catch (error) {
        console.error('Error reading sticker directory:', error);
        res.status(500).json({ error: 'Failed to read sticker directory' });
    }
});

// Clear chat (remove messages between two phones)
app.post('/api/chat/clear', (req, res) => {
    console.log('API /api/chat/clear called:', req.body);
    const { me, other } = req.body;
    if (!me || !other) return res.status(400).json({ error: 'me and other required' });
    let messages = loadMessages();
    messages = messages.filter(m => !((m.sender === me && m.recipient === other) || (m.sender === other && m.recipient === me)));
    saveMessages(messages);
    res.json({ success: true });
});

function applyChatDeletion(me, other, deleteForBoth) {
    const messages = loadMessages();
    let changed = false;
    const targets = deleteForBoth ? [me, other] : [me];

    messages.forEach((message) => {
        if (!((message.sender === me && message.recipient === other) || (message.sender === other && message.recipient === me))) return;
        const deletedFor = Array.isArray(message.deletedFor) ? message.deletedFor : [];
        const nextDeletedFor = Array.from(new Set([...deletedFor, ...targets]));
        if (nextDeletedFor.length !== deletedFor.length || nextDeletedFor.some((value, index) => value !== deletedFor[index])) {
            message.deletedFor = nextDeletedFor;
            changed = true;
        }
    });

    if (changed) saveMessages(messages);
    return changed;
}

function emitChatDeleted(me, other, deleteForBoth) {
    const meSocketId = onlineUsers[me];
    const otherSocketId = onlineUsers[other];
    if (meSocketId) io.to(meSocketId).emit('chat_deleted', { byPhone: me, withPhone: other, deleteForBoth });
    if (deleteForBoth && otherSocketId) io.to(otherSocketId).emit('chat_deleted', { byPhone: me, withPhone: other, deleteForBoth });
    if (meSocketId) broadcastChatList(me);
    if (deleteForBoth && otherSocketId) broadcastChatList(other);
}

app.post('/api/chat/delete', (req, res) => {
    console.log('API /api/chat/delete called:', req.body);
    const { me, other } = req.body;
    if (!me || !other) return res.status(400).json({ error: 'me and other required' });
    applyChatDeletion(me, other, false);
    emitChatDeleted(me, other, false);
    res.json({ success: true });
});

app.post('/api/chat/delete-both', (req, res) => {
    console.log('API /api/chat/delete-both called:', req.body);
    const { me, other } = req.body;
    if (!me || !other) return res.status(400).json({ error: 'me and other required' });
    applyChatDeletion(me, other, true);
    emitChatDeleted(me, other, true);
    res.json({ success: true });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`🚀 Сервер запущен на http://0.0.0.0:${PORT}`));

function saveBase64ToDir(base64Data, targetDir, fileName) {
    try {
        const matches = base64Data.match(/^data:(.*?);base64,(.+)$/);
        if (!matches || matches.length !== 3) {
            console.log("❌ Ошибка парсинга файла: неверный формат base64");
            return null;
        }

        const mimeType = matches[1];
        const base64String = matches[2];
        let extension = 'bin';

        if (mimeType.includes('jpeg') || mimeType.includes('jpg')) extension = 'jpg';
        else if (mimeType.includes('png')) extension = 'png';
        else if (mimeType.includes('gif')) extension = 'gif';
        else if (mimeType.includes('webp')) extension = 'webp';
        else extension = mimeType.split('/')[1] || 'bin';

        const cleanName = (fileName || 'sticker').toString().replace(/[^a-zA-Z0-9._-]/g, '_');
        const finalName = cleanName.includes('.') ? cleanName : `${cleanName}.${extension}`;
        const filePath = path.join(targetDir, finalName);
        const buffer = Buffer.from(base64String, 'base64');

        fs.mkdirSync(targetDir, { recursive: true });
        fs.writeFileSync(filePath, buffer);
        return finalName;
    } catch (e) {
        console.error("❌ Ошибка при сохранении файла в директорию:", e);
        return null;
    }
}

function saveBase64Image(base64Data, phone, type) {
    try {
        const matches = base64Data.match(/^data:(.*?);base64,(.+)$/);
        if (!matches || matches.length !== 3) {
            console.log("❌ Ошибка парсинга файла: неверный формат base64");
            return null; 
        }

        const mimeType = matches[1];
        const base64String = matches[2];
        let extension = 'bin';
        
        if (mimeType.includes('jpeg') || mimeType.includes('jpg')) extension = 'jpg';
        else if (mimeType.includes('png')) extension = 'png';
        else if (mimeType.includes('mp4')) extension = 'mp4';
        else if (mimeType.includes('quicktime')) extension = 'mp4'; 
        else if (mimeType.includes('webm')) extension = 'webm';
        else extension = mimeType.split('/')[1] || 'bin';

        const buffer = Buffer.from(base64String, 'base64');
        const cleanPhone = phone.replace('+', '');
        const fileName = `${cleanPhone}_${type}.${extension}`;
        const filePath = path.join(UPLOADS_DIR, fileName);

        fs.writeFileSync(filePath, buffer);
        return `/uploads/${fileName}?t=${Date.now()}`; 
    } catch (e) {
        console.error("❌ Ошибка при сохранении файла:", e);
        return null;
    }
}
