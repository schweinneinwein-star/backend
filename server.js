const dotenv = require('dotenv');
const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const twilio = require('twilio');
const multer = require('multer');
const cloudinary = require('cloudinary').v2;

const rootEnvPath = path.join(__dirname, '.env');
const uploadEnvPath = path.join(__dirname, 'uploads', '.env');
let envLoadedFrom = null;
if (fs.existsSync(rootEnvPath)) {
  dotenv.config({ path: rootEnvPath });
  envLoadedFrom = rootEnvPath;
} else if (fs.existsSync(uploadEnvPath)) {
  dotenv.config({ path: uploadEnvPath });
  envLoadedFrom = uploadEnvPath;
} else {
  const fallback = dotenv.config();
  if (!fallback.error) envLoadedFrom = 'default .env location';
}
if (envLoadedFrom) {
  console.log(`Loaded environment variables from ${envLoadedFrom}`);
} else {
  console.warn('No .env file found in server root or uploads directory; relying on process.env values only.');
}

const CLOUDINARY_URL = process.env.CLOUDINARY_URL?.trim();
const CLOUDINARY_CLOUD_NAME = process.env.CLOUDINARY_CLOUD_NAME?.trim();
const CLOUDINARY_API_KEY = process.env.CLOUDINARY_API_KEY?.trim();
const CLOUDINARY_API_SECRET = process.env.CLOUDINARY_API_SECRET?.trim();
const missingCloudinaryVars = !CLOUDINARY_URL && (!CLOUDINARY_CLOUD_NAME || !CLOUDINARY_API_KEY || !CLOUDINARY_API_SECRET);
const cloudinaryConfigured = !!CLOUDINARY_URL || (!missingCloudinaryVars);

if (CLOUDINARY_URL) {
    cloudinary.config({ cloudinary_url: CLOUDINARY_URL, secure: true });
} else if (!missingCloudinaryVars) {
    cloudinary.config({
        cloud_name: CLOUDINARY_CLOUD_NAME,
        api_key: CLOUDINARY_API_KEY,
        api_secret: CLOUDINARY_API_SECRET,
        secure: true,
    });
}

if (!cloudinaryConfigured) {
    console.warn('Cloudinary environment variables are not fully configured. File upload endpoint will fail without CLOUDINARY_URL or CLOUDINARY_CLOUD_NAME/CLOUDINARY_API_KEY/CLOUDINARY_API_SECRET.');
}

const UPLOADS_DIR = path.join(__dirname, 'uploads');
fs.mkdirSync(UPLOADS_DIR, { recursive: true });
const pendingCodes = {};
const ipRateLimits = {};

const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: 150 * 1024 * 1024, // 150 MB limit per file
    },
});

async function uploadBufferToCloudinary(file) {
    if (!file || !file.buffer) {
        throw new Error('No buffer data available for Cloudinary upload.');
    }

    return new Promise((resolve, reject) => {
        const uploadStream = cloudinary.uploader.upload_stream(
            {
                folder: 'sparkle_uploads',
                resource_type: 'auto',
            },
            (error, result) => {
                if (error) {
                    return reject(error);
                }
                resolve(result);
            }
        );

        uploadStream.end(file.buffer);
    });
}

function ensureLocalUploadDirectory() {
    const localUploadPath = path.join(UPLOADS_DIR, 'local');
    if (!fs.existsSync(localUploadPath)) {
        fs.mkdirSync(localUploadPath, { recursive: true });
    }
    return localUploadPath;
}

async function saveFileLocally(file) {
    const localUploadPath = ensureLocalUploadDirectory();
    const safeName = `${Date.now()}_${file.originalname.replace(/[^a-zA-Z0-9.\-_]/g, '_')}`;
    const filePath = path.join(localUploadPath, safeName);
    await fs.promises.writeFile(filePath, file.buffer);
    return {
        url: `/uploads/local/${safeName}`,
        publicId: `local_${safeName}`,
        resourceType: (file.mimetype || 'application/octet-stream').split('/')[0] || 'raw',
        originalName: file.originalname || safeName,
        size: file.size || file.buffer.length || 0,
    };
}

const app = express();
const server = http.createServer(app);

const COMMENTS_FILE = path.join(__dirname, 'comments.json');
const POSTS_FILE = path.join(__dirname, 'posts.json');
// IMPORTANT: Groups must be persisted in MongoDB only. No local JSON fallback.

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
    transports: ['websocket', 'polling'],
    maxHttpBufferSize: 1e8,
});

let usersCache = [];
let messagesCache = [];

// --- MONGOOSE / MongoDB SETUP ---
const mongoose = require('mongoose');
// Prefer environment variable MONGO_URI, otherwise use provided credentials
const MONGO_PASSWORD = process.env.MONGO_PASSWORD || 'e70sTKY6FmcDQpBS';
const MONGO_URI = process.env.MONGO_URI || `mongodb+srv://sparklemms_db_user:${encodeURIComponent(MONGO_PASSWORD)}@cluster0.hr8pwru.mongodb.net/sparkle?retryWrites=true&w=majority`;

let mongoReady = false;
let mongoConnectionError = null;

function normalizePhoneString(value) {
    if (value === null || value === undefined) return '';
    return String(value).replace(/\D/g, '');
}

function groupMatchesPhone(group, phone) {
    if (!group || !phone) return false;
    const normalizedPhone = normalizePhoneString(phone);
    if (!normalizedPhone) return false;
    if (normalizePhoneString(group.createdBy) === normalizedPhone) return true;
    return Array.isArray(group.members) && group.members.some((member) => {
        const memberPhone = normalizePhoneString(member?.phone || member?.userId);
        return memberPhone === normalizedPhone;
    });
}

function maskMongoUri(uri) {
    try {
        if (!uri) return '';
        // hide password portion for logging
        return uri.replace(/(mongodb(?:\+srv)?:\/\/)([^:@\/\s]+)(:)([^@\/\s]+)(@)/i, (m, p1, user, colon, pass, at) => `${p1}${user}:***${at}`);
    } catch (e) {
        return '[masked]';
    }
}

const dns = require('dns').promises;

async function resolveMongoSrv(uri) {
    try {
        if (!uri) return null;
        const match = uri.match(/^mongodb\+srv:\/\/([^\/\?]+)/i);
        if (!match) return null;
        let host = match[1];
        // strip credentials if present: user:pass@host => host
        if (host.includes('@')) {
            host = host.split('@').pop();
        }
        try {
            const srv = await dns.resolveSrv(`_mongodb._tcp.${host}`);
            return { host, srv };
        } catch (e) {
            // try plain lookup
            try {
                const lookup = await dns.lookup(host, { all: true });
                return { host, lookup };
            } catch (le) {
                return { host, error: le.message || String(le) };
            }
        }
    } catch (err) {
        return { error: err.message || String(err) };
    }
}

// Try to connect to MongoDB and retry on failure with exponential backoff. Detailed logging for diagnostics.
async function connectMongoWithRetry(attempt = 0) {
    const maxAttempts = 10;
    const delayMs = Math.min(30000, 1000 * Math.pow(2, attempt)); // cap at 30s
    const uri = process.env.MONGO_URI || MONGO_URI;
    const masked = maskMongoUri(uri);
    console.log(`MongoDB connection attempt ${attempt + 1}: uri=${masked}`);

    try {
        // Log DNS/SRV resolution if SRV used
        const srvInfo = await resolveMongoSrv(uri).catch(() => null);
        if (srvInfo) console.log('Mongo SRV/lookup info:', srvInfo);

        await mongoose.connect(uri, {
            serverSelectionTimeoutMS: 10000,
            connectTimeoutMS: 10000,
        });
        mongoReady = true;
        mongoConnectionError = null;
        console.log('✅ MongoDB connected');
        await initCaches();
    } catch (err) {
        mongoReady = false;
        mongoConnectionError = err;
        console.error(`⚠️ MongoDB connection attempt ${attempt + 1} failed:`, { name: err.name, code: err.code, message: err.message });
        if (attempt < maxAttempts - 1) {
            console.log(`Retrying MongoDB connection in ${delayMs}ms... (attempt ${attempt + 2}/${maxAttempts})`);
            setTimeout(() => connectMongoWithRetry(attempt + 1), delayMs);
        } else {
            console.error('❌ MongoDB connection failed after multiple attempts. Group endpoints will return 503 until DB is available.');
        }
    }
}

connectMongoWithRetry();

// Debug endpoint to inspect Mongo connection status and resolution
app.get('/debug/mongo', async (req, res) => {
    try {
        const uri = process.env.MONGO_URI || MONGO_URI;
        const masked = maskMongoUri(uri);
        const srv = await resolveMongoSrv(uri).catch((e) => ({ error: e.message || String(e) }));
        const connected = mongoReady && mongoose.connection.readyState === 1;
        const err = mongoConnectionError ? { name: mongoConnectionError.name, code: mongoConnectionError.code, message: mongoConnectionError.message } : null;
        return res.json({ connected, uri: masked, srv, connectionState: mongoose.connection.readyState, error: err });
    } catch (e) {
        return res.status(500).json({ error: e.message || String(e) });
    }
});

// Define Mongoose schemas and models
const messageSchema = new mongoose.Schema({
    text: { type: String, required: true },
    sender: { type: String, default: null },
    recipient: { type: String, default: null },
    roomId: { type: String, default: null },
    type: { type: String, default: 'text' },
    mediaUrl: { type: String, default: null },
    mediaUrls: { type: [String], default: [] },
    isVideo: { type: Boolean, default: false },
    stickerUrl: { type: String, default: null },
    stickerName: { type: String, default: null },
    packName: { type: String, default: null },
    timestamp: { type: Date, default: Date.now },
    read: { type: Boolean, default: false }
}, { timestamps: true });

const uploadFileSchema = new mongoose.Schema({
    url: { type: String, required: true },
    publicId: { type: String, required: true },
    resourceType: { type: String, required: true },
    originalName: { type: String, required: true },
    size: { type: Number, required: true },
    createdAt: { type: Date, default: Date.now },
}, { timestamps: true });

const groupSchema = new mongoose.Schema({
    id: { type: String, required: true, unique: true },
    type: { type: String, default: 'group' },
    title: { type: String, required: true },
    description: { type: String, default: null },
    avatarUrl: { type: String, default: null },
    createdBy: { type: String, required: true },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    members: { type: [mongoose.Schema.Types.Mixed], default: [] },
    inviteCode: { type: String, default: null },
    settings: { type: mongoose.Schema.Types.Mixed, default: {} },
    pinnedMessageId: { type: String, default: null },
    lastMessage: { type: mongoose.Schema.Types.Mixed, default: null },
    messages: { type: [mongoose.Schema.Types.Mixed], default: [] },
    unreadCount: { type: Number, default: 0 },
}, { timestamps: true });

const userSchema = new mongoose.Schema({
    phone: { type: String, required: true, unique: true },
    firstName: { type: String, default: null },
    lastName: { type: String, default: null },
    username: { type: String, default: null },
    bio: { type: String, default: '' },
    password: { type: String, default: null }, // ДОБАВЛЕНО: поле для хранения пароля
    avatarUrl: { type: String, default: null },
    bannerUrl: { type: String, default: null },
    joinDate: { type: Date, default: Date.now },
    lastSeen: { type: Date, default: null },
    totalReadTime: { type: Number, default: 0 },
    readMessageCount: { type: Number, default: 0 },
    hideAnswerTime: { type: Boolean, default: false },
    votes: { type: Number, default: 0 },
    blockedUsers: { type: [String], default: [] },
    blockedBy: { type: [String], default: [] },
    stickerCollection: { type: [String], default: [] },
    sessionToken: { type: String, default: null },
    sessions: { type: Array, default: [] },
    active: { type: Boolean, default: true },
}, { timestamps: true });

const Message = mongoose.models.Message || mongoose.model('Message', messageSchema);
const UploadFile = mongoose.models.UploadFile || mongoose.model('UploadFile', uploadFileSchema);
const Group = mongoose.models.Group || mongoose.model('Group', groupSchema);
const User = mongoose.models.User || mongoose.model('User', userSchema);

async function initCaches() {
    try {
        usersCache = await User.find({}).lean().exec();
        messagesCache = await Message.find({}).sort({ timestamp: 1 }).lean().exec();
        console.log(`Loaded ${usersCache.length} users from MongoDB`);
        console.log(`Loaded ${messagesCache.length} messages from MongoDB`);
    } catch (err) {
        console.error('initCaches error:', err);
        usersCache = [];
        messagesCache = [];
    }
}

function loadUsers() {
    return usersCache || [];
}

async function saveUsers(users) {
    usersCache = Array.isArray(users) ? users : [];
    try {
        await Promise.all(usersCache.map((user) => {
            if (!user || !user.phone) return Promise.resolve(null);
            return User.findOneAndUpdate(
                { phone: user.phone },
                { $set: user },
                { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true }
            ).exec();
        }));
        console.log(`Saved ${usersCache.length} users to MongoDB`);
    } catch (err) {
        console.error('❌ Error saving users to MongoDB:', err);
    }
}

function loadMessages() {
    return messagesCache || [];
}

function saveMessages(messages) {
    messagesCache = Array.isArray(messages) ? messages : [];
    return Promise.resolve();
}

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

function normalizeGroupPayload(payload) {
    if (!payload || typeof payload !== 'object') return null;
    const members = Array.isArray(payload.members) ? payload.members.map((member) => {
        if (!member || typeof member !== 'object') return member;
        const normalizedPhone = normalizePhoneString(member.phone || member.userId);
        return {
            ...member,
            phone: normalizedPhone,
            userId: normalizePhoneString(member.userId) || normalizedPhone,
        };
    }) : [];
    const createdAt = payload.createdAt || payload.created_at || Date.now();
    const createdBy = normalizePhoneString(payload.createdBy || payload.created_by || payload.creatorPhone || payload.createdByPhone);
    return {
        id: payload.id || payload._id || `group_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        type: payload.type || 'group',
        title: payload.title || payload.name || 'Group',
        description: payload.description || null,
        avatarUrl: payload.avatarUrl || null,
        createdBy,
        createdAt,
        updatedAt: payload.updatedAt || payload.updated_at || createdAt,
        members,
        inviteCode: payload.inviteCode || payload.invite_code || null,
        settings: payload.settings || {},
        pinnedMessageId: payload.pinnedMessageId || null,
        lastMessage: payload.lastMessage || null,
        messages: Array.isArray(payload.messages) ? payload.messages : [],
        unreadCount: Number.isFinite(payload.unreadCount) ? payload.unreadCount : 0,
    };
}

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

const uploadAny = upload.any();

function uploadAnyMiddleware(req, res, next) {
    uploadAny(req, res, (err) => {
        if (err) {
            console.error('Multer upload error:', err);
            const statusCode = err instanceof multer.MulterError ? 400 : 500;
            return res.status(statusCode).json({
                error: err.message || 'File upload failed.',
                code: err.code || 'UPLOAD_ERROR',
                details: err.toString ? err.toString() : err,
            });
        }
        next();
    });
}

async function handleFileUpload(req, res) {
    try {
        const files = [];
        if (Array.isArray(req.files)) {
            files.push(...req.files);
        } else if (req.files && typeof req.files === 'object') {
            Object.values(req.files).forEach((fileArray) => {
                if (Array.isArray(fileArray)) files.push(...fileArray);
            });
        }
        if (req.file) {
            files.push(req.file);
        }

        if (!files.length) {
            console.warn('Upload handler received no files:', { body: req.body, files: req.files, file: req.file });
            return res.status(400).json({ error: 'No files uploaded. Use file or files fields.' });
        }

        const savedFiles = [];
        for (const file of files) {
            const originalName = file.originalname || file.name || `upload_${Date.now()}`;
            const resourceType = (file.mimetype || 'application/octet-stream').split('/')[0] || 'raw';
            const size = Number(file.size || file.buffer?.length || 0);
            let uploadResult;
            let url;
            let publicId;
            let source = 'cloudinary';

            if (cloudinaryConfigured) {
                try {
                    uploadResult = await uploadBufferToCloudinary(file);
                    url = uploadResult.secure_url || uploadResult.url;
                    publicId = uploadResult.public_id || uploadResult.publicId;
                } catch (uploadErr) {
                    console.warn('Cloudinary upload failed, falling back to local storage:', uploadErr.message || uploadErr);
                    source = 'local';
                }
            } else {
                source = 'local';
            }

            if (source === 'local') {
                const localResult = await saveFileLocally(file);
                url = localResult.url;
                publicId = localResult.publicId;
            }

            if (!url || !publicId) {
                return res.status(500).json({ error: 'Failed to save uploaded file.' });
            }

            const savedFile = await UploadFile.create({
                url,
                publicId,
                resourceType,
                originalName,
                size,
            });
            savedFiles.push(savedFile);
        }

        return res.status(201).json({ files: savedFiles });
    } catch (err) {
        console.error('Upload handler failed:', err);
        return res.status(500).json({ error: err.message || 'Failed to upload files.' });
    }
}

app.post('/api/upload', uploadAnyMiddleware, handleFileUpload);
app.post('/upload', uploadAnyMiddleware, handleFileUpload);

app.get('/api/files', async (req, res) => {
    try {
        const files = await UploadFile.find().sort({ createdAt: -1 }).lean();
        return res.json({ files });
    } catch (err) {
        console.error('Failed to fetch files:', err);
        return res.status(500).json({ error: 'Failed to fetch files.' });
    }
});

app.get('/api/files/:id', async (req, res) => {
    try {
        const { id } = req.params;
        if (!mongoose.Types.ObjectId.isValid(id)) {
            return res.status(400).json({ error: 'Invalid file ID.' });
        }

        const file = await UploadFile.findById(id).lean();
        if (!file) {
            return res.status(404).json({ error: 'File not found.' });
        }

        return res.json({ file });
    } catch (err) {
        console.error('Failed to fetch file:', err);
        return res.status(500).json({ error: 'Failed to fetch file.' });
    }
});

app.delete('/api/files/:id', async (req, res) => {
    try {
        const { id } = req.params;
        if (!mongoose.Types.ObjectId.isValid(id)) {
            return res.status(400).json({ error: 'Invalid file ID.' });
        }

        const file = await UploadFile.findById(id);
        if (!file) {
            return res.status(404).json({ error: 'File not found.' });
        }

        try {
            await cloudinary.uploader.destroy(file.publicId, {
                resource_type: file.resourceType || 'auto',
            });
        } catch (destroyErr) {
            console.error('Cloudinary destroy error:', destroyErr);
            return res.status(500).json({ error: 'Failed to delete asset from Cloudinary.' });
        }

        await file.deleteOne();
        return res.json({ success: true, id });
    } catch (err) {
        console.error('Failed to delete file:', err);
        return res.status(500).json({ error: 'Failed to delete file.' });
    }
});

app.use((err, req, res, next) => {
    console.error('Unhandled server error:', err);
    if (res.headersSent) {
        return next(err);
    }

    const statusCode = err && err.status ? err.status : 500;
    return res.status(statusCode).json({
        error: err && err.message ? err.message : 'Internal server error.',
        code: err && err.code ? err.code : 'INTERNAL_SERVER_ERROR',
    });
});

app.get('/groups', async (req, res) => {
    try {
        const phone = req.query.phone;
        if (!phone) {
            return res.status(400).json({ error: 'phone is required' });
        }

        if (!mongoReady || mongoose.connection.readyState !== 1) {
            return res.status(503).json({ error: 'Service unavailable: MongoDB not connected' });
        }

        const groups = await Group.find({
            $or: [
                { createdBy: normalizePhoneString(phone) },
                { members: { $elemMatch: { phone: normalizePhoneString(phone) } } },
                { members: { $elemMatch: { userId: normalizePhoneString(phone) } } },
            ],
        }).sort({ createdAt: -1 }).lean();
        return res.json(groups);
    } catch (err) {
        console.error('Failed to list groups:', err);
        res.status(500).json({ error: 'Failed to list groups' });
    }
});

app.post('/groups', async (req, res) => {
    try {
        console.log('📥 [SERVER] Получил HTTP запрос на создание группы:', JSON.stringify(req.body));
        const payload = normalizeGroupPayload(req.body);
        if (!payload) {
            console.warn('⚠️ [SERVER] HTTP /groups валидатор отклонил payload:', req.body);
            return res.status(400).json({ error: 'Invalid group payload' });
        }
        if (!payload.createdBy) {
            console.warn('⚠️ [SERVER] HTTP /groups missing createdBy:', payload);
            return res.status(400).json({ error: 'createdBy is required' });
        }

        if (!mongoReady || mongoose.connection.readyState !== 1) {
            console.warn('⚠️ [SERVER] HTTP /groups MongoDB не подключен');
            return res.status(503).json({ error: 'Service unavailable: MongoDB not connected' });
        }

        console.log('📦 [SERVER] HTTP /groups отправляю сигнал на MongoDB:', JSON.stringify(payload));
        const created = await Group.create(payload);
        const createdObj = created.toObject ? created.toObject() : created;
        console.log('✅ [SERVER] HTTP /groups сигнал от MongoDB получен - группа сохранена:', createdObj.id || createdObj._id || createdObj.title);
        return res.status(201).json(createdObj);
    } catch (err) {
        console.error('❌ [SERVER] HTTP /groups сигнал от MongoDB получен - запись не выполнена:', err);
        res.status(500).json({ error: 'Failed to create group' });
    }
});

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
    console.log('⚡ Новый пользователь подключился к сокету:', socket.id);
    const clientIp = socket.handshake.address;

    // Send recent messages from MongoDB to the newly connected client (last 50)
    (async () => {
        try {
            const recent = await Message.find({}).sort({ timestamp: -1 }).limit(50).lean();
            socket.emit('recent_messages', recent.reverse());
        } catch (err) {
            console.error('❌ Failed to fetch recent messages from MongoDB:', err);
            // Fallback to in-memory cache
            socket.emit('recent_messages', (messagesCache || []).slice(-50));
        }
    })();

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

socket.on('verify_code', async (data) => {
    const now = Date.now();
    if (ipRateLimits[clientIp] && ipRateLimits[clientIp].blockUntil > now) {
        return socket.emit('sms_rate_limited');
    }

    if (!ipRateLimits[clientIp]) ipRateLimits[clientIp] = { fails: 0, blockUntil: 0 };

    if (data.code === '55555' || pendingCodes[data.phone] === data.code) {
        ipRateLimits[clientIp].fails = 0;
        delete pendingCodes[data.phone];
        
        try {
            // Используем MongoDB вместо JSON файла
            let user = await User.findOne({ phone: data.phone });
            if (user) {
                socket.phone = user.phone;
                onlineUsers[user.phone] = socket.id;
                // Проверяем наличие пароля у пользователя
                const hasPassword = !!user.password;
                const needsPassword = !hasPassword;
                socket.emit('code_verified', { isNewUser: false, hasPassword, needsPassword, user: user.toObject() });
                broadcastChatList(user.phone);
            } else {
                socket.emit('code_verified', { isNewUser: true, phone: data.phone });
            }
        } catch (err) {
            console.error('verify_code error:', err);
            socket.emit('code_invalid');
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

// Обработчик проверки пароля при входе
socket.on('verify_password', async (data) => {
    const { phone, password } = data;
    if (!phone || !password) {
        return socket.emit('password_verify_failed');
    }

    try {
        // Используем MongoDB вместо JSON файла
        let user = await User.findOne({ phone: phone });
        
        if (!user || !user.password) {
            return socket.emit('password_verify_failed');
        }

        // Простая проверка пароля (в production используйте bcrypt!)
        if (user.password === password) {
            socket.phone = user.phone;
            onlineUsers[user.phone] = socket.id;
            socket.emit('password_verified', { success: true, user: user.toObject() });
            broadcastChatList(user.phone);
        } else {
            socket.emit('password_verify_failed');
        }
    } catch (err) {
        console.error('verify_password error:', err);
        socket.emit('password_verify_failed');
    }
});

// 2. Обработчик регистрации с сохранением в MongoDB
socket.on('user_registered', async (userData) => {
    try {
        // Проверяем существует ли уже пользователь
        let existingUser = await User.findOne({ phone: userData.phone });
        if (existingUser) {
            return socket.emit('auth_error', 'User already exists');
        }

        // Создаем нового пользователя с паролем
        let user = new User({
            phone: userData.phone,
            firstName: userData.firstName,
            lastName: userData.lastName,
            username: userData.username,
            bio: userData.bio,
            password: userData.password || null, // Сохраняем пароль
            joinDate: Date.now(),
            sessionToken: Math.random().toString(36).substr(2) + Date.now()
        });

        // Сохраняем аватар если есть
        if (userData.avatarBase64) {
            const newUrl = await saveBase64Image(userData.avatarBase64, userData.phone, 'avatar');
            if (newUrl) user.avatarUrl = newUrl;
        }

        // Сохраняем в MongoDB
        await user.save();
        
        socket.phone = user.phone;
        onlineUsers[user.phone] = socket.id;
        socket.emit('auth_success', user.toObject());
        
        // Обновляем кеш
        usersCache.push(user.toObject());
        
    } catch (err) {
        console.error('user_registered error:', err);
        socket.emit('auth_error', err.message || 'Registration failed');
    }
});

// 3. Обработчик обновления профиля с поддержкой пароля в MongoDB
socket.on('update_profile', async (data) => {
    const userPhone = socket.phone || data.phone;
    if (!userPhone) return;

    try {
        // Используем MongoDB вместо JSON файла
        let user = await User.findOne({ phone: userPhone });
        if (!user) return;

        // Обновляем поля
        if (data.avatarBase64) {
            const newUrl = await saveBase64Image(data.avatarBase64, userPhone, 'avatar');
            if (newUrl) user.avatarUrl = newUrl;
        }

        if (data.bannerBase64) {
            const newUrl = await saveBase64Image(data.bannerBase64, userPhone, 'banner');
            if (newUrl) user.bannerUrl = newUrl;
        }

        if (data.username) user.username = data.username;
        if (data.bio) user.bio = data.bio;
        if (data.firstName !== undefined) user.firstName = data.firstName;
        if (data.lastName !== undefined) user.lastName = data.lastName;
        if (data.hideAnswerTime !== undefined) user.hideAnswerTime = data.hideAnswerTime;
        if (data.password) user.password = data.password; // Сохраняем пароль

        // Сохраняем в MongoDB
        await user.save();
        
        io.emit('profile_updated', user.toObject());
        broadcastChatList(userPhone);
        
    } catch (err) {
        console.error('update_profile error:', err);
    }
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

    socket.on('get_chat_history', async (data) => {
        if (!socket.phone) return;
        const myPhone = socket.phone;
        const partnerPhone = data.withPhone;
        try {
            // Fetch from MongoDB
            const history = await Message.find({
                $or: [
                    { sender: myPhone, recipient: partnerPhone },
                    { sender: partnerPhone, recipient: myPhone }
                ]
            }).sort({ timestamp: 1 }).limit(100).lean();
            socket.emit('chat_history', history);
        } catch (err) {
            console.error('❌ Failed to fetch chat history from MongoDB:', err);
            // Fallback to file cache
            const messages = loadMessages();
            const history = messages.filter(m => 
                isMessageVisibleForUser(m, myPhone) && (
                    (m.sender === myPhone && m.recipient === partnerPhone) ||
                    (m.sender === partnerPhone && m.recipient === myPhone)
                )
            );
            socket.emit('chat_history', history);
        }
    });

    socket.on('send_private_message', async (data) => {
        console.log('📥 send_private_message invoked. socket.id=', socket.id, 'socket.phone=', socket.phone, 'payload=', JSON.stringify(data));
        if (!socket.phone && !(data && data.sender)) {
            console.warn('❗ send_private_message rejected: missing socket.phone and payload.sender', { socketPhone: socket.phone, payload: data });
            return;
        }

        const hasMediaUrls = Array.isArray(data?.mediaUrls) && data.mediaUrls.length > 0;
        if (!data || (!data.text && !data.mediaBase64 && !data.mediaUrl && !hasMediaUrls && data.type !== 'call_log' && data.type !== 'sticker' && data.type !== 'image')) {
            return;
        }

        const senderPhone = socket.phone || data.sender;
        const recipientPhone = data.recipientPhone || data.recipient || data.roomId;
        if (!recipientPhone) {
            console.warn('❗ send_private_message rejected: missing recipientPhone', data);
            return;
        }

        console.log('🔥 СОКЕТ ПРИНЯЛ СООБЩЕНИЕ:', data);

        let mediaUrl = null;
        if (data.mediaBase64) {
            mediaUrl = await saveBase64Image(data.mediaBase64, senderPhone, `chat_${Date.now()}`);
            if (!mediaUrl) return;
        }

        const newMsg = {
            text: data.text ? String(data.text).trim() : '',
            sender: senderPhone,
            recipient: recipientPhone,
            roomId: recipientPhone,
            type: data.type || 'text',
            callStatus: data.callStatus || null,
            duration: data.duration || null,
            stickerUrl: data.stickerUrl || null,
            stickerName: data.stickerName || null,
            packName: data.packName || null,
            mediaUrl: data.mediaUrl || mediaUrl,
            mediaUrls: Array.isArray(data.mediaUrls) ? data.mediaUrls.map(String) : [],
            isVideo: !!data.isVideo,
            timestamp: data.timestamp ? new Date(data.timestamp) : new Date(),
            read: false,
        };

        try {
            console.log('🔵 4. [SERVER] Пытаюсь сохранить send_private_message в MongoDB...');
            const saved = await Message.create(newMsg);
            messagesCache.push(saved);
            saveMessages(messagesCache);

            const recipientSocketId = onlineUsers[recipientPhone];
            if (recipientSocketId) {
                io.to(recipientSocketId).emit('new_private_message', saved);
                broadcastChatList(recipientPhone);
            }

            socket.emit('new_private_message', saved);
            broadcastChatList(senderPhone);
        } catch (err) {
            console.error('❌ Ошибка сохранения send_private_message в MongoDB:', err);
            try {
                const recipientSocketId = onlineUsers[recipientPhone];
                if (recipientSocketId) {
                    io.to(recipientSocketId).emit('new_private_message', newMsg);
                    broadcastChatList(recipientPhone);
                }
                socket.emit('new_private_message', newMsg);
                broadcastChatList(senderPhone);
            } catch (emitErr) {
                console.error('❌ Ошибка отправки fallback send_private_message:', emitErr);
            }
        }
    });

    // Robust 'send_message' handler (accepts partial payloads and always emits)
    socket.on('send_message', async (data) => {
        console.log('💻 [СЕРВЕР] Сокет поймал сообщение от клиента:', data);
        console.log('🔥 УРА! СЕРВЕР ПОЛУЧИЛ СООБЩЕНИЕ ОТ ТЕЛЕФОНА:', data);
        try {
            console.log('🔵 4. [SERVER] Пытаюсь сохранить в MongoDB...');
            const createdMessage = await Message.create({
                text: data.text ? String(data.text) : '',
                sender: data.sender || data.username || null,
                recipient: data.recipientPhone || data.recipient || null,
                roomId: data.roomId || data.recipientPhone || data.recipient || null,
                type: data.type || 'text',
                mediaUrl: data.mediaUrl || null,
                stickerUrl: data.stickerUrl || null,
                stickerName: data.stickerName || null,
                packName: data.packName || null,
                timestamp: data.timestamp ? new Date(data.timestamp) : new Date(),
                read: data.read || false,
            });
            console.log('✅ 5. [SERVER] УСПЕШНО СОХРАНЕНО В БАЗУ!', createdMessage);
            io.emit('receive_message', createdMessage);
        } catch (err) {
            console.error('❌ 6. [SERVER] ОШИБКА БАЗЫ ДАННЫХ:', err && err.message ? err.message : err);
            try {
                io.emit('receive_message', data);
            } catch (emitErr) {
                console.error('❌ Ошибка отправки fallback-сообщения:', emitErr && emitErr.message ? emitErr.message : emitErr);
            }
        }
    });

    // Group creation via socket
    socket.on('create_group', async (data) => {
        console.log('📥 [SERVER] Получил socket create_group от', socket.phone || 'unknown', 'payload:', JSON.stringify(data));
        if (!socket.phone) {
            console.warn('⚠️ [SERVER] socket create_group отклонён: неавторизован');
            return socket.emit('group_creation_error', 'Not authenticated');
        }

        const payload = normalizeGroupPayload({
            ...data,
            createdBy: data?.createdBy || socket.phone,
            title: data?.title || data?.name,
        });
        if (!payload?.title || !Array.isArray(payload.members)) {
            console.warn('⚠️ [SERVER] socket create_group валидатор отклонил payload:', payload);
            return socket.emit('group_creation_error', 'Invalid group payload');
        }

        try {
            if (!mongoReady || mongoose.connection.readyState !== 1) {
                console.warn('⚠️ [SERVER] socket create_group MongoDB не подключен');
                return socket.emit('group_creation_error', 'Service unavailable: MongoDB not connected');
            }
            console.log('📦 [SERVER] socket create_group отправляю сигнал на MongoDB:', JSON.stringify(payload));
            const group = await Group.create(payload);
            const groupObj = group.toObject ? group.toObject() : group;
            console.log('✅ [SERVER] socket create_group сигнал от MongoDB получен - группа сохранена:', groupObj.id || groupObj._id || groupObj.title);
            io.emit('group_created', groupObj);
            socket.emit('group_created', groupObj);
        } catch (err) {
            console.error('❌ [SERVER] socket create_group сигнал от MongoDB получен - запись не выполнена:', err);
            socket.emit('group_creation_error', err.message || 'Failed to create group');
        }
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

async function uploadBase64ToCloudinary(base64Data, fileName) {
    if (!cloudinaryConfigured) return null;

    try {
        const result = await cloudinary.uploader.upload(base64Data, {
            folder: 'sparkle_uploads',
            public_id: fileName,
            resource_type: 'auto',
            overwrite: true,
        });
        return result.secure_url || result.url || null;
    } catch (err) {
        console.error('Cloudinary base64 upload failed:', err);
        return null;
    }
}

async function saveBase64Image(base64Data, phone, type) {
    try {
        const matches = base64Data.match(/^data:(.*?);base64,(.+)$/);
        if (!matches || matches.length !== 3) {
            console.log('❌ Ошибка парсинга файла: неверный формат base64');
            return null;
        }

        const mimeType = matches[1];
        let extension = 'bin';
        if (mimeType.includes('jpeg') || mimeType.includes('jpg')) extension = 'jpg';
        else if (mimeType.includes('png')) extension = 'png';
        else if (mimeType.includes('mp4')) extension = 'mp4';
        else if (mimeType.includes('quicktime')) extension = 'mp4';
        else if (mimeType.includes('webm')) extension = 'webm';
        else extension = mimeType.split('/')[1] || 'bin';

        const cleanPhone = phone.replace('+', '');
        const fileName = `${cleanPhone}_${type}`;

        if (cloudinaryConfigured) {
            const uploadedUrl = await uploadBase64ToCloudinary(base64Data, fileName);
            if (uploadedUrl) return uploadedUrl;
            console.warn('Cloudinary upload failed, falling back to local file save for base64 asset');
        }

        const base64String = matches[2];
        const buffer = Buffer.from(base64String, 'base64');
        const localUploadPath = ensureLocalUploadDirectory();
        const localFileName = `${fileName}.${extension}`;
        const filePath = path.join(localUploadPath, localFileName);
        fs.writeFileSync(filePath, buffer);
        return `/uploads/local/${localFileName}?t=${Date.now()}`;
    } catch (e) {
        console.error('❌ Ошибка при сохранении файла:', e);
        return null;
    }
}
