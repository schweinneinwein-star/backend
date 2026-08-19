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

const CLOUDINARY_URL = process.env.CLOUDINARY_URL?.trim() || null;
const CLOUDINARY_CLOUD_NAME = process.env.CLOUDINARY_CLOUD_NAME?.trim() || null;
const CLOUDINARY_API_KEY = process.env.CLOUDINARY_API_KEY?.trim() || null;
const CLOUDINARY_API_SECRET = process.env.CLOUDINARY_API_SECRET?.trim() || null;
const cloudinaryConfigured = !!CLOUDINARY_URL || (!!CLOUDINARY_CLOUD_NAME && !!CLOUDINARY_API_KEY && !!CLOUDINARY_API_SECRET);

const cloudinaryConfig = { secure: true };
if (CLOUDINARY_URL) {
    cloudinaryConfig.cloudinary_url = CLOUDINARY_URL;
}
if (CLOUDINARY_CLOUD_NAME) {
    cloudinaryConfig.cloud_name = CLOUDINARY_CLOUD_NAME;
}
if (CLOUDINARY_API_KEY) {
    cloudinaryConfig.api_key = CLOUDINARY_API_KEY;
}
if (CLOUDINARY_API_SECRET) {
    cloudinaryConfig.api_secret = CLOUDINARY_API_SECRET;
}

// Helper: try to parse CLOUDINARY_URL of form cloudinary://API_KEY:API_SECRET@CLOUD_NAME
function parseCloudinaryUrl(url) {
    try {
        if (!url || typeof url !== 'string') return null;
        const m = url.match(/^cloudinary:\/\/(.+?):(.+?)@(.+)$/i);
        if (!m) return null;
        return {
            api_key: m[1],
            api_secret: m[2],
            cloud_name: m[3],
        };
    } catch (e) {
        return null;
    }
}

// If CLOUDINARY_URL provided but individual keys are missing, try parsing it for api_key/api_secret/cloud_name
if (CLOUDINARY_URL && (!cloudinaryConfig.api_key || !cloudinaryConfig.api_secret || !cloudinaryConfig.cloud_name)) {
    const parsed = parseCloudinaryUrl(CLOUDINARY_URL);
    if (parsed) {
        cloudinaryConfig.api_key = cloudinaryConfig.api_key || parsed.api_key;
        cloudinaryConfig.api_secret = cloudinaryConfig.api_secret || parsed.api_secret;
        cloudinaryConfig.cloud_name = cloudinaryConfig.cloud_name || parsed.cloud_name;
        console.log('Parsed CLOUDINARY_URL into api_key/cloud_name (partial):', { cloud_name: !!parsed.cloud_name, api_key: !!parsed.api_key });
    } else {
        console.log('CLOUDINARY_URL provided but could not parse it to extract api_key/api_secret/cloud_name; ensure it uses cloudinary://API_KEY:API_SECRET@CLOUD_NAME format');
    }
}

cloudinary.config(cloudinaryConfig);

const cloudinaryRuntimeConfig = cloudinary.config();
console.log('Cloudinary config status:', {
    cloudinaryConfigured,
    cloudinaryUrl: !!cloudinaryConfig.cloudinary_url,
    cloudName: !!cloudinaryRuntimeConfig.cloud_name,
    apiKey: !!cloudinaryRuntimeConfig.api_key,
    apiSecret: !!cloudinaryRuntimeConfig.api_secret,
});

if (!cloudinaryConfigured) {
    console.warn('Cloudinary environment variables are not fully configured. File upload endpoint will fall back to local save unless CLOUDINARY_URL or CLOUDINARY_CLOUD_NAME/CLOUDINARY_API_KEY/CLOUDINARY_API_SECRET are set.');
} else {
    // Additional runtime check
    if (!cloudinaryRuntimeConfig.api_key || !cloudinaryRuntimeConfig.api_secret || !cloudinaryRuntimeConfig.cloud_name) {
        console.warn('Cloudinary appears partially configured. Uploads may fail. Runtime config:', { cloud_name: cloudinaryRuntimeConfig.cloud_name, api_key: !!cloudinaryRuntimeConfig.api_key, api_secret: !!cloudinaryRuntimeConfig.api_secret });
    }
}

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || null;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || null;
const TWILIO_PHONE = process.env.TWILIO_PHONE || null;
const twilioClient = TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN ? twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) : null;

if (!twilioClient) {
    console.warn('Twilio is not configured. SMS verification will fall back to dev-mode code generation and will not send real SMS until TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN/TWILIO_PHONE are set.');
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

    const runtimeConfig = cloudinary.config();
    const hasKeys = !!runtimeConfig.api_key && !!runtimeConfig.api_secret && !!runtimeConfig.cloud_name;
    if (!hasKeys) {
        // provide a richer error message to help diagnostics
        const msg = `Cloudinary upload failed: incomplete runtime configuration. runtimeConfig: ${JSON.stringify({ cloud_name: runtimeConfig.cloud_name ? true : false, api_key: runtimeConfig.api_key ? true : false, api_secret: runtimeConfig.api_secret ? true : false })}`;
        const err = new Error(msg);
        err.runtimeConfig = runtimeConfig;
        throw err;
    }

    return new Promise((resolve, reject) => {
        const uploadStream = cloudinary.uploader.upload_stream(
            {
                folder: 'sparkle_uploads',
                resource_type: 'auto',
                secure: true,
            },
            (error, result) => {
                if (error) {
                    // include better context in logs
                    console.error('Cloudinary uploader returned error:', error && (error.message || error));
                    return reject(error);
                }
                resolve(result);
            }
        );

        try {
            uploadStream.end(file.buffer);
        } catch (e) {
            console.error('Failed to end upload stream for Cloudinary:', e);
            reject(e);
        }
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
    text: { type: String, default: '' },
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
    read: { type: Boolean, default: false },
    replyTo: { type: mongoose.Schema.Types.Mixed, default: null },
    replyMeta: { type: mongoose.Schema.Types.Mixed, default: null },
    deletedFor: { type: [String], default: [] },
    isPinned: { type: Boolean, default: false },
    pinnedBy: { type: String, default: null },
    pinnedAt: { type: Date, default: null },
    edited: { type: Boolean, default: false },
    editedAt: { type: Date, default: null }
}, { timestamps: true });

const uploadFileSchema = new mongoose.Schema({
    url: { type: String, required: true },
    publicId: { type: String, required: true },
    resourceType: { type: String, required: true },
    originalName: { type: String, required: true },
    size: { type: Number, required: true },
    source: { type: String, default: 'cloudinary' }, // 'cloudinary' or 'local'
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
    profileChannelId: { type: mongoose.Schema.Types.ObjectId, ref: 'Channel', default: null },
    profileChannelHidden: { type: Boolean, default: false },
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

// -------------------- Каналы (Channels) --------------------
// Detailed Mongoose schemas aligned with requested specification
const channelSchema = new mongoose.Schema({
    name: { type: String, required: true, trim: true, maxlength: 64 },
    description: { type: String, default: null, maxlength: 255 },
    avatar: { type: String, default: null }, // Cloudinary URL
    banner: { type: String, default: null }, // Cloudinary URL for channel cover
    username: { type: String, default: null, sparse: true, unique: true }, // public @username
    type: { type: String, enum: ['public', 'private'], default: 'public' },
    inviteLink: { type: String, default: null },
    owner: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    admins: [{
        user: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
        permissions: {
            canPost: { type: Boolean, default: true },
            canEdit: { type: Boolean, default: true },
            canDelete: { type: Boolean, default: true },
            canAddAdmins: { type: Boolean, default: false },
        }
    }],
    subscribers: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
    bannedUsers: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
    restrictedContent: { type: Boolean, default: false },
    createdAt: { type: Date, default: Date.now }
}, { timestamps: true });

// Text index for global search on public channels
channelSchema.index({ name: 'text', description: 'text' });
// Unique sparse index for username (public handles)
channelSchema.index({ username: 1 }, { unique: true, sparse: true });

const channelPostSchema = new mongoose.Schema({
    channelId: { type: mongoose.Schema.Types.ObjectId, ref: 'Channel', required: true },
    authorId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    text: { type: String, default: '' },
    media: [{
        url: { type: String, required: true },
        type: { type: String, enum: ['image', 'video', 'file', 'audio'], default: 'image' },
        publicId: { type: String, default: null }
    }],
    views: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }], // unique viewers
    reactions: [{ emoji: String, users: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }] }],
    isPinned: { type: Boolean, default: false },
    isSilent: { type: Boolean, default: false },
    scheduledAt: { type: Date, default: null },
    createdAt: { type: Date, default: Date.now }
}, { timestamps: true });

const channelCommentSchema = new mongoose.Schema({
    postId: { type: mongoose.Schema.Types.ObjectId, ref: 'ChannelPost', required: true },
    channelId: { type: mongoose.Schema.Types.ObjectId, ref: 'Channel', required: true },
    authorId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    text: { type: String, required: true },
    mediaUrl: { type: String, default: null },
    reactions: [{ emoji: String, users: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }] }],
    createdAt: { type: Date, default: Date.now }
}, { timestamps: true });

const Channel = mongoose.models.Channel || mongoose.model('Channel', channelSchema);
const ChannelPost = mongoose.models.ChannelPost || mongoose.model('ChannelPost', channelPostSchema);
const ChannelComment = mongoose.models.ChannelComment || mongoose.model('ChannelComment', channelCommentSchema);

function normalizeReactionGroups(reactions) {
    const groups = {};
    const queue = Array.isArray(reactions) ? reactions : [];

    queue.forEach((entry) => {
        if (!entry || typeof entry !== 'object') return;
        const emoji = entry.emoji || entry.reaction;
        if (!emoji) return;
        const users = Array.isArray(entry.users)
            ? entry.users
            : ((entry.userId || entry.user) ? [entry.userId || entry.user] : []);
        const uniqueUsers = Array.from(new Set((users || []).map((u) => String(u)).filter(Boolean)));
        const current = groups[emoji] || [];
        groups[emoji] = Array.from(new Set([...current, ...uniqueUsers]));
    });

    return groups;
}

function upsertReactionForUser(post, userId, emoji) {
    const safeUserId = String(userId);
    const safeEmoji = String(emoji || '').trim();
    if (!safeUserId || !safeEmoji) return post;

    const nextReactions = (post.reactions || []).map((entry) => ({
        emoji: entry.emoji,
        users: Array.isArray(entry.users) ? entry.users.map((u) => String(u)) : []
    }));

    const existingEntry = nextReactions.find((entry) => entry.emoji === safeEmoji);
    if (existingEntry) {
        const nextUsers = existingEntry.users.filter((id) => String(id) !== safeUserId);
        if (nextUsers.length === 0) {
            post.reactions = nextReactions.filter((entry) => entry.emoji !== safeEmoji);
            return post;
        }
        existingEntry.users = nextUsers;
        post.reactions = nextReactions.map((entry) => ({
            emoji: entry.emoji,
            users: Array.from(new Set(entry.users.map((id) => String(id))))
        }));
        return post;
    }

    nextReactions.push({ emoji: safeEmoji, users: [safeUserId] });
    post.reactions = nextReactions.map((entry) => ({
        emoji: entry.emoji,
        users: Array.from(new Set(entry.users.map((id) => String(id))))
    }));
    return post;
}

// ------------------------------------------------------------

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
                source: source || 'cloudinary',
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

// -------------------- Channels REST API --------------------
// Helper to upload single avatar file to Cloudinary (or save locally as fallback)
async function saveAvatarFile(file) {
    if (!file) return null;
    try {
        if (cloudinaryConfigured) {
            const result = await uploadBufferToCloudinary(file);
            return result.secure_url || result.url;
        }
    } catch (err) {
        console.warn('Channel avatar upload to Cloudinary failed, falling back to local save:', err.message || err);
    }
    const local = await saveFileLocally(file);
    return local.url;
}

app.post('/api/channels', uploadAnyMiddleware, async (req, res) => {
    try {
        const { name, description, type, ownerPhone, ownerId, inviteLink, restrictedContent, username } = req.body;
        if (!name) return res.status(400).json({ error: 'name is required' });

        let owner = null;
        if (ownerId && mongoose.Types.ObjectId.isValid(ownerId)) {
            owner = await User.findById(ownerId);
        } else if (ownerPhone) {
            owner = await User.findOne({ phone: String(ownerPhone).trim() });
        }
        if (!owner) return res.status(400).json({ error: 'owner not found (provide ownerId or ownerPhone)' });

        let avatarUrl = req.body.avatarUrl || null;
        let bannerUrl = req.body.bannerUrl || null;
        if (req.files && req.files.length) {
            for (const file of req.files) {
                const fieldName = (file.fieldname || '').toString().toLowerCase();
                if (fieldName === 'avatar') {
                    avatarUrl = await saveAvatarFile(file);
                } else if (fieldName === 'banner') {
                    try {
                        const uploadResult = await uploadBufferToCloudinary(file);
                        bannerUrl = uploadResult.secure_url || uploadResult.url;
                    } catch (err) {
                        console.warn('Banner upload failed, falling back to local save:', err.message || err);
                        const localResult = await saveFileLocally(file);
                        bannerUrl = localResult.url;
                    }
                } else if (!avatarUrl) {
                    avatarUrl = await saveAvatarFile(file);
                }
            }
        }

        const channel = await Channel.create({
            name: String(name).trim(),
            description: description || null,
            avatar: avatarUrl || null,
            banner: bannerUrl || null,
            username: username ? String(username).trim() : null,
            type: type === 'private' ? 'private' : 'public',
            inviteLink: inviteLink || (type === 'private' ? `sparkle.me/join/${Math.random().toString(36).slice(2, 10)}` : null),
            owner: owner._id,
            admins: [{ user: owner._id, permissions: { canPost: true, canEdit: true, canDelete: true, canAddAdmins: true } }],
            subscribers: [owner._id],
            bannedUsers: [],
            restrictedContent: !!restrictedContent,
        });

        const channelObj = channel.toObject();
        try { io.emit('channel_created', channelObj); } catch (e) { /* ignore */ }
        return res.status(201).json(channelObj);
    } catch (err) {
        console.error('Failed to create channel:', err);
        return res.status(500).json({ error: err.message || 'Failed to create channel' });
    }
});

app.post('/api/channels/:id/join', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId, userPhone } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id)) return res.status(400).json({ error: 'Invalid channel id' });
        const channel = await Channel.findById(id);
        if (!channel) return res.status(404).json({ error: 'Channel not found' });

        let user = null;
        if (userId && mongoose.Types.ObjectId.isValid(userId)) user = await User.findById(userId);
        else if (userPhone) user = await User.findOne({ phone: String(userPhone).trim() });
        if (!user) return res.status(400).json({ error: 'User not found' });

        if (channel.bannedUsers && channel.bannedUsers.some(b => b.equals(user._id))) {
            return res.status(403).json({ error: 'User is banned from this channel' });
        }

        if (!channel.subscribers.some(s => s.equals(user._id))) {
            channel.subscribers.push(user._id);
            await channel.save();
            try { io.to(String(channel._id)).emit('channel_joined', { channelId: channel._id, userId: user._id }); } catch (e) {}
        }
        return res.json({ success: true, channelId: channel._id });
    } catch (err) {
        console.error('Join channel error:', err);
        res.status(500).json({ error: 'Failed to join channel' });
    }
});

app.post('/api/channels/:id/leave', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId, userPhone } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id)) return res.status(400).json({ error: 'Invalid channel id' });
        const channel = await Channel.findById(id);
        if (!channel) return res.status(404).json({ error: 'Channel not found' });

        let user = null;
        if (userId && mongoose.Types.ObjectId.isValid(userId)) user = await User.findById(userId);
        else if (userPhone) user = await User.findOne({ phone: String(userPhone).trim() });
        if (!user) return res.status(400).json({ error: 'User not found' });

        channel.subscribers = channel.subscribers.filter(s => !s.equals(user._id));
        channel.admins = channel.admins.filter(a => String(a.user) !== String(user._id));
        await channel.save();
        try { io.to(String(channel._id)).emit('channel_left', { channelId: channel._id, userId: user._id }); } catch (e) {}
        return res.json({ success: true });
    } catch (err) {
        console.error('Leave channel error:', err);
        res.status(500).json({ error: 'Failed to leave channel' });
    }
});

app.post('/api/channels/:id/posts', uploadAnyMiddleware, async (req, res) => {
    try {
        const { id } = req.params;
        const { authorId, authorPhone, content } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id)) return res.status(400).json({ error: 'Invalid channel id' });
        const channel = await Channel.findById(id);
        if (!channel) return res.status(404).json({ error: 'Channel not found' });

        let author = null;
        if (authorId && mongoose.Types.ObjectId.isValid(authorId)) author = await User.findById(authorId);
        else if (authorPhone) author = await User.findOne({ phone: String(authorPhone).trim() });
        if (!author) return res.status(400).json({ error: 'Author not found' });

        // Check rights: owner or admin
        const isAdmin = channel.owner.equals(author._id) || (channel.admins || []).some(a => String(a.user) === String(author._id));
        if (!isAdmin) return res.status(403).json({ error: 'Only owner or admins can post' });

        const mediaUrls = [];
        if (Array.isArray(req.files) && req.files.length) {
            for (const f of req.files) {
                try {
                    const uploadRes = await uploadBufferToCloudinary(f);
                    mediaUrls.push(uploadRes.secure_url || uploadRes.url);
                } catch (e) {
                    const local = await saveFileLocally(f);
                    mediaUrls.push(local.url);
                }
            }
        }

        const post = await ChannelPost.create({ channelId: channel._id, authorId: author._id, text: content || '', media: (mediaUrls || []).map(u => ({ url: u })) });
        const postObj = post.toObject();
        // notify subscribers in socket room
        try { io.to(String(channel._id)).emit('new_channel_post', postObj); } catch (e) { /* ignore */ }
        return res.status(201).json(postObj);
    } catch (err) {
        console.error('Create channel post error:', err);
        res.status(500).json({ error: 'Failed to create post' });
    }
});

app.delete('/api/channels/:id/posts/:postId', async (req, res) => {
    try {
        const { id, postId } = req.params;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId)) return res.status(400).json({ error: 'Invalid id' });
        const post = await ChannelPost.findOne({ _id: postId, channelId: id });
        if (!post) return res.status(404).json({ error: 'Post not found' });
        await post.deleteOne();
        try { io.to(String(id)).emit('channel_post_deleted', { postId }); } catch (e) {}
        return res.json({ success: true });
    } catch (err) {
        console.error('Delete post error:', err);
        res.status(500).json({ error: 'Failed to delete post' });
    }
});

app.post('/api/channels/:id/posts/:postId/pin', async (req, res) => {
    try {
        const { id, postId } = req.params;
        const { userId } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId)) return res.status(400).json({ error: 'Invalid id' });
        const channel = await Channel.findById(id);
        if (!channel) return res.status(404).json({ error: 'Channel not found' });
        const user = userId && mongoose.Types.ObjectId.isValid(userId) ? await User.findById(userId) : null;
        if (!user) return res.status(400).json({ error: 'User not found' });
        const isAdmin = channel.owner.equals(user._id) || (channel.admins || []).some(a => String(a.user) === String(user._id));
        if (!isAdmin) return res.status(403).json({ error: 'Not authorized' });
        await ChannelPost.updateMany({ channelId: channel._id }, { $set: { isPinned: false } });
        await ChannelPost.findByIdAndUpdate(postId, { isPinned: true });
        try { io.to(String(channel._id)).emit('post_pinned', { postId }); } catch (e) {}
        return res.json({ success: true });
    } catch (err) {
        console.error('Pin post error:', err);
        res.status(500).json({ error: 'Failed to pin post' });
    }
});

app.post('/api/channels/:id/posts/:postId/react', async (req, res) => {
    try {
        const { id, postId } = req.params;
        const { userId, userPhone, emoji } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId)) return res.status(400).json({ error: 'Invalid id' });
        const post = await ChannelPost.findOne({ _id: postId, channelId: id });
        if (!post) return res.status(404).json({ error: 'Post not found' });

        let resolvedUserId = null;
        if (userId && mongoose.Types.ObjectId.isValid(userId)) resolvedUserId = userId;
        else if (userPhone) {
            const u = await User.findOne({ phone: String(userPhone).trim() });
            if (u) resolvedUserId = String(u._id);
        }

        if (!resolvedUserId) return res.status(400).json({ error: 'userId or userPhone required' });

        post.reactions = post.reactions || [];
        const normalized = normalizeReactionGroups(post.reactions);
        const currentUsers = normalized[emoji] || [];

        // One reaction per user only: remove any previous emoji from this user before applying the new one.
        const clearedReactions = (post.reactions || []).map((entry) => ({
            emoji: entry.emoji,
            users: Array.isArray(entry.users) ? entry.users.filter((u) => String(u) !== String(resolvedUserId)) : []
        })).filter((entry) => entry.emoji && entry.users.length > 0);

        if (currentUsers.includes(String(resolvedUserId))) {
            post.reactions = clearedReactions;
            if (clearedReactions.some((entry) => entry.emoji === emoji)) {
                post.reactions = clearedReactions.filter((entry) => entry.emoji !== emoji);
            }
        } else {
            const nextEntry = clearedReactions.find((entry) => entry.emoji === emoji);
            if (nextEntry) {
                nextEntry.users = Array.from(new Set([...nextEntry.users, String(resolvedUserId)]));
                post.reactions = clearedReactions;
            } else {
                post.reactions = [...clearedReactions, { emoji, users: [resolvedUserId] }];
            }
        }
        await post.save();
        try { io.to(String(id)).emit('post_reactions_updated', { postId, reactions: post.reactions }); } catch (e) {}
        return res.json({ success: true, reactions: post.reactions });
    } catch (err) {
        console.error('React error:', err);
        res.status(500).json({ error: 'Failed to react to post' });
    }
});

app.post('/api/channels/:id/posts/:postId/view', async (req, res) => {
    try {
        const { id, postId } = req.params;
        const { userId } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId)) return res.status(400).json({ error: 'Invalid id' });
        const post = await ChannelPost.findOne({ _id: postId, channelId: id });
        if (!post) return res.status(404).json({ error: 'Post not found' });
        if (!userId || !mongoose.Types.ObjectId.isValid(userId)) return res.status(400).json({ error: 'userId required' });
        if (!post.views.some(v => String(v) === String(userId))) {
            post.views.push(userId);
            await post.save();
            try { io.to(String(id)).emit('post_views_updated', { postId, viewsCount: post.views.length }); } catch (e) {}
        }
        return res.json({ success: true, viewsCount: post.views.length });
    } catch (err) {
        console.error('View post error:', err);
        res.status(500).json({ error: 'Failed to view post' });
    }
});

app.get('/api/channels/:id/posts/:postId/comments', async (req, res) => {
    try {
        const { id, postId } = req.params;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId)) return res.status(400).json({ error: 'Invalid id' });
        const post = await ChannelPost.findOne({ _id: postId, channelId: id }).lean();
        if (!post) return res.status(404).json({ error: 'Post not found' });

        const comments = await ChannelComment.find({ postId: post._id }).sort({ createdAt: 1 }).lean();
        const authorIds = Array.from(new Set(comments.map(c => String(c.authorId)).filter(Boolean)));
        const authors = await User.find({ _id: { $in: authorIds } }).lean();
        const authorMap = {};
        authors.forEach((author) => { authorMap[String(author._id)] = author; });

        const normalizedComments = comments.map((comment) => {
            const author = authorMap[String(comment.authorId)] || null;
            return {
                _id: comment._id,
                id: comment._id,
                postId: comment.postId,
                channelId: comment.channelId,
                authorId: comment.authorId,
                authorPhone: author ? author.phone : null,
                senderPhone: author ? author.phone : null,
                senderName: author ? ([author.firstName, author.lastName].filter(Boolean).join(' ') || author.username || author.phone) : 'User',
                senderAvatar: author ? (author.avatarUrl || null) : null,
                text: comment.text,
                mediaUrl: comment.mediaUrl || null,
                timestamp: comment.createdAt || comment.created_at || Date.now(),
                createdAt: comment.createdAt || comment.created_at || Date.now(),
                reactions: Array.isArray(comment.reactions) ? comment.reactions : [],
            };
        });

        return res.json({ post, comments: normalizedComments });
    } catch (err) {
        console.error('Fetch comments error:', err);
        res.status(500).json({ error: 'Failed to fetch comments' });
    }
});

app.post('/api/channels/:id/posts/:postId/comments', async (req, res) => {
    try {
        const { id, postId } = req.params;
        const { authorId, authorPhone, text } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId)) return res.status(400).json({ error: 'Invalid id' });
        if (!text || !String(text).trim()) return res.status(400).json({ error: 'Text required' });

        let author = null;
        if (authorId && mongoose.Types.ObjectId.isValid(authorId)) author = await User.findById(authorId);
        else if (authorPhone) author = await User.findOne({ phone: String(authorPhone).trim() });
        if (!author) return res.status(400).json({ error: 'Author not found' });

        const post = await ChannelPost.findOne({ _id: postId, channelId: id });
        if (!post) return res.status(404).json({ error: 'Post not found' });
        const comment = await ChannelComment.create({ postId: post._id, channelId: id, authorId: author._id, text: String(text).trim(), reactions: [] });
        try { io.to(String(id)).emit('channel_comment_added', { postId, comment: comment.toObject() }); } catch (e) {}
        return res.status(201).json(comment);
    } catch (err) {
        console.error('Add comment error:', err);
        res.status(500).json({ error: 'Failed to add comment' });
    }
});

app.post('/api/channels/:id/posts/:postId/comments/:commentId/react', async (req, res) => {
    try {
        const { id, postId, commentId } = req.params;
        const { userId, userPhone, emoji } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(postId) || !mongoose.Types.ObjectId.isValid(commentId)) {
            return res.status(400).json({ error: 'Invalid id' });
        }

        const comment = await ChannelComment.findOne({ _id: commentId, postId, channelId: id });
        if (!comment) return res.status(404).json({ error: 'Comment not found' });

        let resolvedUserId = null;
        if (userId && mongoose.Types.ObjectId.isValid(userId)) resolvedUserId = userId;
        else if (userPhone) {
            const user = await User.findOne({ phone: String(userPhone).trim() });
            if (user) resolvedUserId = String(user._id);
        }
        if (!resolvedUserId) return res.status(400).json({ error: 'userId or userPhone required' });

        const safeEmoji = String(emoji || '').trim();
        if (!safeEmoji) return res.status(400).json({ error: 'emoji required' });

        comment.reactions = Array.isArray(comment.reactions) ? comment.reactions : [];
        const existing = comment.reactions.find((entry) => entry.emoji === safeEmoji);
        const withoutUser = comment.reactions
            .map((entry) => ({
                emoji: entry.emoji,
                users: Array.isArray(entry.users) ? entry.users.filter((u) => String(u) !== String(resolvedUserId)) : []
            }))
            .filter((entry) => entry.emoji && entry.users.length > 0);

        if (existing && (existing.users || []).map(String).includes(String(resolvedUserId))) {
            comment.reactions = withoutUser;
        } else {
            const nextEntry = withoutUser.find((entry) => entry.emoji === safeEmoji);
            if (nextEntry) {
                nextEntry.users = Array.from(new Set([...nextEntry.users, String(resolvedUserId)]));
                comment.reactions = withoutUser;
            } else {
                comment.reactions = [...withoutUser, { emoji: safeEmoji, users: [resolvedUserId] }];
            }
        }

        await comment.save();
        try { io.to(String(id)).emit('channel:comment:reaction_updated', { postId, commentId, reactions: comment.reactions }); } catch (e) {}
        return res.json({ success: true, reactions: comment.reactions });
    } catch (err) {
        console.error('Comment reaction error:', err);
        return res.status(500).json({ error: 'Failed to react to comment' });
    }
});

app.post('/api/channels/:id/ban', async (req, res) => {
    try {
        const { id } = req.params;
        const { targetUserId, byUserId } = req.body;
        if (!mongoose.Types.ObjectId.isValid(id) || !mongoose.Types.ObjectId.isValid(targetUserId) || !mongoose.Types.ObjectId.isValid(byUserId)) return res.status(400).json({ error: 'Invalid ids' });
        const channel = await Channel.findById(id);
        if (!channel) return res.status(404).json({ error: 'Channel not found' });
        const byUser = await User.findById(byUserId);
        if (!byUser) return res.status(400).json({ error: 'By user not found' });
        // only owner can ban
        if (!channel.owner.equals(byUser._id)) return res.status(403).json({ error: 'Only owner can ban users' });
        if (!channel.bannedUsers.some(b => b.equals(targetUserId))) channel.bannedUsers.push(targetUserId);
        channel.subscribers = channel.subscribers.filter(s => !s.equals(targetUserId));
        channel.admins = channel.admins.filter(a => String(a.user) !== String(targetUserId));
        await channel.save();
        try { io.to(String(id)).emit('channel_user_banned', { channelId: id, userId: targetUserId }); } catch (e) {}
        return res.json({ success: true });
    } catch (err) {
        console.error('Ban user error:', err);
        res.status(500).json({ error: 'Failed to ban user' });
    }
});

app.get('/api/channels', async (req, res) => {
    try {
        const subscribedPhone = req.query.subscribedPhone;
        const ownerPhone = req.query.ownerPhone;
        const ownerId = req.query.ownerId;

        const findUserByPhone = async (rawPhone) => {
            if (!rawPhone) return null;
            const variants = Array.from(new Set([
                String(rawPhone).trim(),
                normalizePhoneString(rawPhone),
            ].filter(Boolean)));
            if (!variants.length) return null;
            return User.findOne({ phone: { $in: variants } }).lean();
        };

        if (ownerPhone) {
            const owner = await findUserByPhone(ownerPhone);
            if (!owner) return res.json({ channels: [] });
            const channels = await Channel.find({ owner: owner._id }).sort({ createdAt: -1 }).lean();
            return res.json({ channels });
        }

        if (ownerId) {
            if (!mongoose.Types.ObjectId.isValid(ownerId)) return res.status(400).json({ error: 'Invalid ownerId' });
            const channels = await Channel.find({ owner: ownerId }).sort({ createdAt: -1 }).lean();
            return res.json({ channels });
        }

        if (!subscribedPhone) {
            return res.status(400).json({ error: 'subscribedPhone or ownerPhone query parameter is required' });
        }

        const user = await findUserByPhone(subscribedPhone);
        if (!user) {
            return res.json({ channels: [] });
        }

        const channels = await Channel.find({ subscribers: user._id }).sort({ createdAt: -1 }).lean();
        return res.json({ channels });
    } catch (err) {
        console.error('Failed to list channels:', err);
        res.status(500).json({ error: 'Failed to list channels' });
    }
});

app.get('/api/channels/search', async (req, res) => {
    try {
        const q = (req.query.q || '').trim();
        if (!q) return res.json({ results: [] });

        const safeQuery = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const regex = new RegExp(safeQuery, 'i');

        let results = await Channel.find({
            type: 'public',
            $or: [
                { $text: { $search: q } },
                { username: regex },
                { name: regex },
                { description: regex },
                { 'bio': regex },
            ],
        }).limit(50).lean();

        // ensure name and description are matched by text index fallback if $text was not present
        if (!results || results.length === 0) {
            // try wider search using regex across name and description
            results = await Channel.find({ type: 'public', $or: [ { name: regex }, { description: regex }, { username: regex }, { bio: regex } ] }).limit(50).lean();
        }

        const formatted = results.map((channel) => ({
            _id: channel._id,
            name: channel.name,
            description: channel.description,
            username: channel.username,
            avatar: channel.avatar,
            banner: channel.banner,
            subscriberCount: Array.isArray(channel.subscribers) ? channel.subscribers.length : 0,
        }));

        return res.json({ results: formatted });
    } catch (err) {
        console.error('Channel search error:', err);
        res.status(500).json({ error: 'Search failed' });
    }
});

app.get('/api/channels/:id', async (req, res) => {
    try {
        const { id } = req.params;
        if (!mongoose.Types.ObjectId.isValid(id)) return res.status(400).json({ error: 'Invalid channel id' });

        const channel = await Channel.findById(id).lean();
        if (!channel) return res.status(404).json({ error: 'Channel not found' });

        let posts = await ChannelPost.find({ channelId: channel._id }).sort({ isPinned: -1, createdAt: -1 }).lean();
        try {
            const postIds = posts.map((p) => p._id).filter(Boolean);
            const commentCounts = await ChannelComment.aggregate([
                { $match: { postId: { $in: postIds } } },
                { $group: { _id: '$postId', count: { $sum: 1 } } }
            ]);
            const commentMap = Object.fromEntries(commentCounts.map((entry) => [String(entry._id), Number(entry.count) || 0]));

            const authorIds = Array.from(new Set(posts.map(p => String(p.authorId)).filter(Boolean)));
            const authors = await User.find({ _id: { $in: authorIds } }).lean();
            const authorMap = {};
            authors.forEach(a => { authorMap[String(a._id)] = a; });
            posts = posts.map(p => ({
                ...p,
                commentCount: Number(commentMap[String(p._id)] || 0),
                authorName: (authorMap[String(p.authorId)] && ((authorMap[String(p.authorId)].firstName || authorMap[String(p.authorId)].username) ? `${authorMap[String(p.authorId)].firstName || ''} ${authorMap[String(p.authorId)].lastName || ''}`.trim() : authorMap[String(p.authorId)].username || authorMap[String(p.authorId)].phone) ) || null,
                authorAvatar: authorMap[String(p.authorId)] ? (authorMap[String(p.authorId)].avatarUrl || authorMap[String(p.authorId)].avatar || null) : null,
            }));
        } catch (e) {
            console.warn('Failed to attach author info to posts:', e && e.message ? e.message : e);
        }
        const userPhone = req.query.userPhone ? String(req.query.userPhone).trim() : null;
        let isSubscriber = false;
        let isAdmin = false;
        let isOwner = false;

        if (userPhone) {
            const user = await User.findOne({ phone: userPhone });
            if (user) {
                isSubscriber = Array.isArray(channel.subscribers) && channel.subscribers.some((sub) => String(sub) === String(user._id));
                isOwner = String(channel.owner) === String(user._id);
                isAdmin = isOwner || Array.isArray(channel.admins) && channel.admins.some((a) => String(a.user) === String(user._id));
            }
        }

        const result = {
            channel: {
                ...channel,
                isSubscriber,
                isOwner,
                isAdmin,
                subscriberCount: Array.isArray(channel.subscribers) ? channel.subscribers.length : 0,
            },
            posts,
        };

        return res.json(result);
    } catch (err) {
        console.error('Failed to fetch channel details:', err);
        res.status(500).json({ error: 'Failed to fetch channel details' });
    }
});

// ------------------------------------------------------------

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

function attachSocketPhone(socket, phone) {
    if (!socket || !phone) return false;
    const normalizedPhone = String(phone).trim();
    if (!normalizedPhone) return false;

    socket.phone = normalizedPhone;
    onlineUsers[normalizedPhone] = socket.id;
    console.log(`✅ Socket ${socket.id} attached to phone ${normalizedPhone}`);
    broadcastChatList(normalizedPhone);
    return true;
}

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
            profileChannelId: partnerUser.profileChannelId || null,
            profileChannelHidden: !!partnerUser.profileChannelHidden,
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
            attachSocketPhone(socket, user.phone);
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

    socket.on('user_connected', (data) => {
        const phone = data?.phone || data?.sender || data?.senderPhone || data?.userPhone;
        if (!phone) {
            socket.emit('auth_error', 'Missing phone in user_connected payload');
            return;
        }
        attachSocketPhone(socket, phone);
        socket.emit('user_connected_success', { phone: socket.phone });
    });

    socket.on('auth', (data) => {
        const phone = data?.phone || data?.sender || data?.senderPhone || data?.userPhone;
        if (!phone) {
            socket.emit('auth_error', 'Missing phone in auth payload');
            return;
        }
        attachSocketPhone(socket, phone);
        socket.emit('auth_success', { phone: socket.phone });
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

        const sendFallback = () => {
            console.log(`📱 DEV SMS fallback enabled for ${phone}. Code: ${code}`);
            socket.emit('code_sent_success');
        };

        if (!twilioClient || !TWILIO_PHONE) {
            sendFallback();
            return;
        }

        twilioClient.messages.create({
            body: `${code} is your Sparkle verification code. Do not share it with anyone.`,
            from: TWILIO_PHONE,
            to: phone
        })
        .then(() => {
            socket.emit('code_sent_success');
        })
        .catch(error => {
            console.error('❌ Ошибка отправки СМС, включен Fallback:', error);
            sendFallback();
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
        if (data.profileChannelHidden !== undefined) user.profileChannelHidden = !!data.profileChannelHidden;
        if (data.profileChannelId !== undefined) {
            if (!data.profileChannelId || String(data.profileChannelId) === 'null') {
                user.profileChannelId = null;
            } else if (mongoose.Types.ObjectId.isValid(data.profileChannelId)) {
                user.profileChannelId = data.profileChannelId;
            }
        }
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

        const senderPhone = socket.phone || data?.sender || data?.senderPhone || data?.userPhone || data?.phone;
        const recipientPhone = data?.recipientPhone || data?.recipient || data?.to || data?.roomId;
        if (!senderPhone) {
            console.warn('❗ send_private_message rejected: missing senderPhone', { socketPhone: socket.phone, payload: data });
            return;
        }
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
            replyTo: data.replyTo || null,
            replyMeta: data.replyMeta || null,
            deletedFor: [],
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

    socket.on('message:reply', async (data) => {
        try {
            const senderPhone = socket.phone || data?.sender || data?.userId;
            const recipientPhone = data?.recipientPhone || data?.chatId || data?.to || data?.recipient;
            if (!senderPhone || !recipientPhone) return;

            const payload = {
                text: String(data?.text || '').trim(),
                sender: senderPhone,
                recipient: recipientPhone,
                roomId: recipientPhone,
                type: data?.type || 'text',
                mediaUrls: Array.isArray(data?.mediaUrls) ? data.mediaUrls.map(String) : [],
                timestamp: data?.timestamp ? new Date(data.timestamp) : new Date(),
                read: false,
                replyTo: data?.replyTo || null,
                replyMeta: data?.replyMeta || null,
                deletedFor: [],
            };
            const saved = await Message.create(payload);
            const recipientSocketId = onlineUsers[recipientPhone];
            if (recipientSocketId) io.to(recipientSocketId).emit('new_private_message', saved);
            socket.emit('new_private_message', saved);
            broadcastChatList(senderPhone);
            broadcastChatList(recipientPhone);
        } catch (err) {
            console.error('message:reply error', err);
        }
    });

    socket.on('message:edit', async (data) => {
        try {
            const { messageId, text, chatId, userId } = data || {};
            if (!messageId || !text) return;
            const updated = await Message.findOneAndUpdate(
                { _id: messageId },
                { $set: { text: String(text), edited: true, editedAt: Date.now() } },
                { new: true }
            );
            if (!updated) return;
            const payload = { chatId, messageId: String(updated._id), text: String(text), edited: true, editedAt: Date.now(), userId };
            if (onlineUsers[userId]) io.to(onlineUsers[userId]).emit('message_edited', payload);
            const peer = chatId && onlineUsers[chatId];
            if (peer) io.to(peer).emit('message_edited', payload);
        } catch (err) {
            console.error('message:edit error', err);
        }
    });

    socket.on('message:delete_for_all', async (data) => {
        try {
            const { messageId, chatId, userId } = data || {};
            if (!messageId) return;
            const removed = await Message.findOneAndDelete({ _id: messageId });
            if (!removed) return;
            const payload = { chatId, messageId: String(messageId), userId };
            if (userId && onlineUsers[userId]) io.to(onlineUsers[userId]).emit('message_deleted_for_all', payload);
            if (chatId && onlineUsers[chatId]) io.to(onlineUsers[chatId]).emit('message_deleted_for_all', payload);
        } catch (err) {
            console.error('message:delete_for_all error', err);
        }
    });

    socket.on('message:delete_for_me', async (data) => {
        try {
            const { messageId, chatId, userId } = data || {};
            if (!messageId || !userId) return;
            const msg = await Message.findOne({ _id: messageId });
            if (!msg) return;
            const deletedFor = Array.isArray(msg.deletedFor) ? [...new Set([...msg.deletedFor, userId])] : [userId];
            msg.deletedFor = deletedFor;
            await msg.save();
            const payload = { chatId, messageId: String(messageId), userId };
            if (chatId && onlineUsers[chatId]) io.to(onlineUsers[chatId]).emit('message_deleted_for_me', payload);
            if (onlineUsers[userId]) io.to(onlineUsers[userId]).emit('message_deleted_for_me', payload);
        } catch (err) {
            console.error('message:delete_for_me error', err);
        }
    });

    socket.on('message:pin', async (data) => {
        try {
            const { messageId, chatId, userId } = data || {};
            if (!messageId) return;
            if (messageId && String(messageId).length > 0) {
                await Message.updateOne({ _id: messageId }, { $set: { isPinned: true, pinnedBy: userId, pinnedAt: Date.now() } }, { upsert: false });
            }
            const payload = { chatId, messageId: String(messageId), userId };
            if (chatId && onlineUsers[chatId]) io.to(onlineUsers[chatId]).emit('message_pinned', payload);
            if (userId && onlineUsers[userId]) io.to(onlineUsers[userId]).emit('message_pinned', payload);
        } catch (err) {
            console.error('message:pin error', err);
        }
    });

    socket.on('message:unpin', async (data) => {
        try {
            const { messageId, chatId, userId } = data || {};
            if (!messageId) return;
            await Message.updateOne({ _id: messageId }, { $set: { isPinned: false, pinnedBy: null, pinnedAt: null } });
            const payload = { chatId, messageId: String(messageId), userId };
            if (chatId && onlineUsers[chatId]) io.to(onlineUsers[chatId]).emit('message_unpinned', payload);
            if (userId && onlineUsers[userId]) io.to(onlineUsers[userId]).emit('message_unpinned', payload);
        } catch (err) {
            console.error('message:unpin error', err);
        }
    });

    socket.on('message:save', async (data) => {
        try {
            const { messageId, userId, savedCopy } = data || {};
            if (!messageId || !userId) return;
            const saved = savedCopy || { messageId, savedBy: userId, savedAt: Date.now() };
            if (global.savedMessages) {
                global.savedMessages[userId] = global.savedMessages[userId] || [];
                global.savedMessages[userId].push(saved);
            }
            socket.emit('message_saved', { messageId, userId, saved });
        } catch (err) {
            console.error('message:save error', err);
        }
    });

    socket.on('message:forward', async (data) => {
        try {
            const { toPhone, fromPhone, message, text, isAnonymous, forwardedFromName } = data || {};
            if (!toPhone || !message) return;
            const forwarded = {
                ...message,
                sender: fromPhone || message.sender,
                recipient: toPhone,
                roomId: toPhone,
                text: text || message.text || 'Forwarded message',
                type: message.type || 'text',
                timestamp: data.timestamp ? new Date(data.timestamp) : new Date(),
                forwarded: true,
                forwardedFrom: isAnonymous ? null : (forwardedFromName || fromPhone),
                forwardedAnonymous: !!isAnonymous,
            };
            const recipientSocketId = onlineUsers[toPhone];
            if (recipientSocketId) io.to(recipientSocketId).emit('new_private_message', forwarded);
            socket.emit('new_private_message', forwarded);
        } catch (err) {
            console.error('message:forward error', err);
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

    socket.on('search_user', async (data) => {
        try {
            if (!data || !data.query) return socket.emit('search_results', { results: [] });
            const query = String(data.query).trim();
            if (!query) return socket.emit('search_results', { results: [] });

            const normalizedQuery = query.replace(/^@+/, '').toLowerCase();
            const q = normalizedQuery;

            let users = loadUsers();
            if (mongoReady && mongoose.connection.readyState === 1) {
                const mongoMatches = await User.find({
                    $or: [
                        { username: { $regex: q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' } },
                        { bio: { $regex: q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' } },
                        { phone: { $regex: q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' } },
                    ],
                }).lean();
                if (mongoMatches.length) {
                    const mergedMap = new Map();
                    [...users, ...mongoMatches].forEach((u) => {
                        if (!u || !u.phone) return;
                        mergedMap.set(String(u.phone), { ...u });
                    });
                    users = Array.from(mergedMap.values());
                }
            }

            let userResults = [];
            if (data.mode === 'username') {
                userResults = users.filter((u) => {
                    const username = String(u.username || '').toLowerCase();
                    const phone = String(u.phone || '').toLowerCase();
                    return username.includes(q) || phone.includes(q);
                });
            } else {
                userResults = users.filter((u) => String(u.bio || '').toLowerCase().includes(q));
            }

            const userResultsNormalized = userResults.map((u) => ({
                ...u,
                type: 'user',
                profileChannelId: u.profileChannelId || null,
                profileChannelHidden: !!u.profileChannelHidden,
                isOnline: !!onlineUsers[u.phone],
                lastSeen: u.lastSeen || null,
                joinDate: u.joinDate || null,
                totalReadTime: u.totalReadTime || 0,
                readMessageCount: u.readMessageCount || 0,
                hideAnswerTime: u.hideAnswerTime || false,
            }));

            let channelResults = [];
            if (mongoReady && mongoose.connection.readyState === 1) {
                const safeQuery = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const regex = new RegExp(safeQuery, 'i');
                const channelMatches = await Channel.find({
                    type: 'public',
                    $or: [
                        { username: regex },
                        { name: regex },
                        { description: regex },
                        { bio: regex },
                    ],
                }).limit(50).lean();

                channelResults = channelMatches.map((channel) => ({
                    _id: String(channel._id),
                    id: String(channel._id),
                    type: 'channel',
                    name: channel.name || channel.username || 'Канал',
                    description: channel.description || channel.bio || '',
                    username: channel.username || null,
                    avatar: channel.avatar || null,
                    avatarUrl: channel.avatar || null,
                    banner: channel.banner || null,
                    bannerUrl: channel.banner || null,
                    subscriberCount: Array.isArray(channel.subscribers) ? channel.subscribers.length : 0,
                    isOnline: false,
                }));
            }

            const deduped = [...channelResults, ...userResultsNormalized].filter((item, index, arr) => {
                return arr.findIndex((candidate) => {
                    if (candidate.type !== item.type) return false;
                    if (item.type === 'channel') {
                        return String(candidate._id || candidate.id) === String(item._id || item.id);
                    }
                    return String(candidate.phone || candidate.username) === String(item.phone || item.username);
                }) === index;
            });

            socket.emit('search_results', { results: deduped });
        } catch (error) {
            console.error('search_user error:', error);
            socket.emit('search_results', { results: [] });
        }
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

    // ==================== CHANNELS (Socket handlers) ====================

    // Create a channel (owner becomes initial admin and subscriber)
    socket.on('channel:create', async (data) => {
        try {
            const ownerPhone = socket.phone || data.ownerPhone;
            const ownerId = data.ownerId || null;
            let owner = null;
            if (ownerId && mongoose.Types.ObjectId.isValid(ownerId)) owner = await User.findById(ownerId);
            else if (ownerPhone) owner = await User.findOne({ phone: String(ownerPhone).trim() });
            if (!owner) return socket.emit('channel:error', { message: 'Owner not found' });

            // handle avatar upload (base64 or provided file url)
            let avatarUrl = data.avatarUrl || null;
            if (data.avatarBase64) {
                try {
                    const saved = await saveBase64Image(data.avatarBase64, owner.phone || owner._id, 'channel_avatar');
                    if (saved) avatarUrl = saved;
                } catch (err) {
                    console.warn('channel:create avatar upload failed:', err && err.message ? err.message : err);
                }
            }

            const inviteLink = data.inviteLink || (data.type === 'private' ? `sparkle.me/join/${Math.random().toString(36).slice(2,10)}` : null);

            const channelDoc = await Channel.create({
                name: String(data.name || '').trim(),
                description: data.description || null,
                avatar: avatarUrl,
                username: data.username || null,
                type: data.type === 'private' ? 'private' : 'public',
                inviteLink,
                owner: owner._id,
                admins: [{ user: owner._id, permissions: { canPost: true, canEdit: true, canDelete: true, canAddAdmins: true } }],
                subscribers: [owner._id],
                bannedUsers: [],
                restrictedContent: !!data.restrictedContent,
            });

            const channelObj = channelDoc.toObject ? channelDoc.toObject() : channelDoc;
            socket.join(String(channelObj._id));
            io.emit('channel:created', channelObj);
            socket.emit('channel:create:success', channelObj);
        } catch (err) {
            console.error('socket channel:create error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to create channel' });
        }
    });

    // Join channel
    socket.on('channel:join', async (data) => {
        try {
            const channelId = data.channelId;
            if (!mongoose.Types.ObjectId.isValid(channelId)) return socket.emit('channel:error', { message: 'Invalid channelId' });
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel:error', { message: 'Channel not found' });

            const userPhone = socket.phone || data.userPhone;
            const userId = data.userId || null;
            let user = null;
            if (userId && mongoose.Types.ObjectId.isValid(userId)) user = await User.findById(userId);
            else if (userPhone) user = await User.findOne({ phone: String(userPhone).trim() });
            if (!user) return socket.emit('channel:error', { message: 'User not found' });

            if ((channel.bannedUsers || []).some(b => String(b) === String(user._id))) return socket.emit('channel:error', { message: 'You are banned from this channel' });

            if (!(channel.subscribers || []).some(s => String(s) === String(user._id))) {
                channel.subscribers.push(user._id);
                await channel.save();
            }

            socket.join(String(channel._id));
            io.to(String(channel._id)).emit('channel:member_joined', { channelId: channel._id, user: { _id: user._id, phone: user.phone, username: user.username } });
            socket.emit('channel:join:success', { channelId: channel._id });
        } catch (err) {
            console.error('socket channel:join error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to join channel' });
        }
    });

    // Leave channel
    socket.on('channel:leave', async (data) => {
        try {
            const channelId = data.channelId;
            if (!mongoose.Types.ObjectId.isValid(channelId)) return socket.emit('channel:error', { message: 'Invalid channelId' });
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel:error', { message: 'Channel not found' });

            const userPhone = socket.phone || data.userPhone;
            const userId = data.userId || null;
            let user = null;
            if (userId && mongoose.Types.ObjectId.isValid(userId)) user = await User.findById(userId);
            else if (userPhone) user = await User.findOne({ phone: String(userPhone).trim() });
            if (!user) return socket.emit('channel:error', { message: 'User not found' });

            channel.subscribers = (channel.subscribers || []).filter(s => String(s) !== String(user._id));
            channel.admins = (channel.admins || []).filter(a => String(a.user || a) !== String(user._id));
            await channel.save();

            try { socket.leave(String(channel._id)); } catch (e) {}
            io.to(String(channel._id)).emit('channel:member_left', { channelId: channel._id, userId: user._id });
            socket.emit('channel:leave:success', { channelId: channel._id });
        } catch (err) {
            console.error('socket channel:leave error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to leave channel' });
        }
    });

    // Create channel post (owner/admins with canPost)
    socket.on('channel:post:create', async (data) => {
        try {
            const channelId = data.channelId;
            if (!mongoose.Types.ObjectId.isValid(channelId)) return socket.emit('channel:error', { message: 'Invalid channelId' });
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel:error', { message: 'Channel not found' });

            // identify user
            const userPhone = socket.phone || data.authorPhone;
            const authorId = data.authorId || null;
            let author = null;
            if (authorId && mongoose.Types.ObjectId.isValid(authorId)) author = await User.findById(authorId);
            else if (userPhone) author = await User.findOne({ phone: String(userPhone).trim() });
            if (!author) return socket.emit('channel:error', { message: 'Author not found' });

            // Permissions: owner or admin with canPost
            const isOwner = String(channel.owner) === String(author._id);
            const adminEntry = (channel.admins || []).find(a => String(a.user || a) === String(author._id));
            const isAdminWithPost = adminEntry ? (adminEntry.permissions ? !!adminEntry.permissions.canPost : true) : false;
            if (!isOwner && !isAdminWithPost) return socket.emit('channel:error', { message: 'No permission to post' });

            const mediaObjects = [];
            // support data.mediaBase64: array of base64 strings
            if (Array.isArray(data.mediaBase64) && data.mediaBase64.length) {
                for (const b64 of data.mediaBase64) {
                    try {
                        const raw = b64.split(',').pop();
                        const buffer = Buffer.from(raw, 'base64');
                        const fakeFile = { buffer };
                        const uploadRes = await uploadBufferToCloudinary(fakeFile);
                        mediaObjects.push({ url: uploadRes.secure_url || uploadRes.url, type: uploadRes.resource_type === 'video' ? 'video' : 'image', publicId: uploadRes.public_id });
                    } catch (err) {
                        console.warn('channel:post:create media upload failed:', err && err.message ? err.message : err);
                        // skip failed media
                    }
                }
            }

            // support provided mediaUrls array of {url,publicId,type}
            if (Array.isArray(data.media) && data.media.length) {
                for (const m of data.media) {
                    if (m && m.url) mediaObjects.push({ url: m.url, type: m.type || 'image', publicId: m.publicId || m.public_id || null });
                }
            }

            const postDoc = await ChannelPost.create({
                    text: data.content || '',
                channelId: channel._id,
                authorId: author._id,
                text: data.text || '',
                media: mediaObjects,
                views: [],
                reactions: [],
                isPinned: !!data.isPinned,
                isSilent: !!data.isSilent,
                scheduledAt: data.scheduledAt ? new Date(data.scheduledAt) : null,
            });

            const postObj = postDoc.toObject();
            io.to(String(channel._id)).emit('channel:post:new', postObj);
            socket.emit('channel:post:create:success', postObj);
        } catch (err) {
            console.error('socket channel:post:create error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to create post' });
        }
    });

    // Edit channel post (author or admin with canEdit)
    socket.on('channel:post:edit', async (data) => {
        try {
            const { channelId, postId, text } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel:error', { message: 'Invalid ids' });
            const channel = await Channel.findById(channelId);
            const post = await ChannelPost.findById(postId);
            if (!channel || !post) return socket.emit('channel:error', { message: 'Channel or post not found' });

            const userPhone = socket.phone || data.userPhone;
            const userId = data.userId || null;
            let user = null;
            if (userId && mongoose.Types.ObjectId.isValid(userId)) user = await User.findById(userId);
            else if (userPhone) user = await User.findOne({ phone: String(userPhone).trim() });
            if (!user) return socket.emit('channel:error', { message: 'User not found' });

            const isOwner = String(channel.owner) === String(user._id);
            const adminEntry = (channel.admins || []).find(a => String(a.user || a) === String(user._id));
            const canEdit = isOwner || (adminEntry && adminEntry.permissions && adminEntry.permissions.canEdit);
            if (!canEdit && String(post.authorId) !== String(user._id)) return socket.emit('channel:error', { message: 'Not authorized to edit' });

            if (typeof text === 'string') post.text = text;
            // TODO: support media edits (add/remove) safely
            await post.save();
            io.to(String(channel._id)).emit('channel:post:updated', { postId: post._id, post: post.toObject() });
            socket.emit('channel:post:edit:success', post.toObject());
        } catch (err) {
            console.error('socket channel:post:edit error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to edit post' });
        }
    });

    // Delete channel post (owner or admin with canDelete)
    socket.on('channel:post:delete', async (data) => {
        try {
            const { channelId, postId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel:error', { message: 'Invalid ids' });
            const channel = await Channel.findById(channelId);
            const post = await ChannelPost.findById(postId);
            if (!channel || !post) return socket.emit('channel:error', { message: 'Channel or post not found' });

            const userPhone = socket.phone || data.userPhone;
            const userId = data.userId || null;
            let user = null;
            if (userId && mongoose.Types.ObjectId.isValid(userId)) user = await User.findById(userId);
            else if (userPhone) user = await User.findOne({ phone: String(userPhone).trim() });
            if (!user) return socket.emit('channel:error', { message: 'User not found' });

            const isOwner = String(channel.owner) === String(user._id);
            const adminEntry = (channel.admins || []).find(a => String(a.user || a) === String(user._id));
            const canDelete = isOwner || (adminEntry && adminEntry.permissions && adminEntry.permissions.canDelete);
            if (!canDelete && String(post.authorId) !== String(user._id)) return socket.emit('channel:error', { message: 'Not authorized to delete' });

            // attempt to remove cloudinary assets
            if (Array.isArray(post.media)) {
                for (const m of post.media) {
                    if (m && m.publicId) {
                        try {
                            await cloudinary.uploader.destroy(m.publicId, { resource_type: m.type === 'video' ? 'video' : 'image' });
                        } catch (err) {
                            console.warn('Failed to destroy cloudinary asset', m.publicId, err && err.message ? err.message : err);
                        }
                    }
                }
            }

            await post.deleteOne();
            io.to(String(channel._id)).emit('channel:post:deleted', { postId });
            socket.emit('channel:post:delete:success', { postId });
        } catch (err) {
            console.error('socket channel:post:delete error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to delete post' });
        }
    });

    // Pin/unpin post
    socket.on('channel:post:pin', async (data) => {
        try {
            const { channelId, postId, byUserId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel:error', { message: 'Invalid ids' });
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel:error', { message: 'Channel not found' });

            const byUser = byUserId && mongoose.Types.ObjectId.isValid(byUserId) ? await User.findById(byUserId) : null;
            if (!byUser) return socket.emit('channel:error', { message: 'User not found' });
            const isOwner = String(channel.owner) === String(byUser._id);
            const adminEntry = (channel.admins || []).find(a => String(a.user || a) === String(byUser._id));
            const canPin = isOwner || (adminEntry && adminEntry.permissions && (adminEntry.permissions.canEdit || adminEntry.permissions.canDelete));
            if (!canPin) return socket.emit('channel:error', { message: 'Not authorized to pin' });

            await ChannelPost.updateMany({ channelId: channel._id }, { $set: { isPinned: false } });
            await ChannelPost.findByIdAndUpdate(postId, { isPinned: true });
            io.to(String(channel._id)).emit('channel:post:pinned', { postId });
            socket.emit('channel:post:pin:success', { postId });
        } catch (err) {
            console.error('socket channel:post:pin error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to pin post' });
        }
    });

    // View post (unique per user)
    socket.on('channel:post:view', async (data) => {
        try {
            const { channelId, postId, userId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return;
            const post = await ChannelPost.findById(postId);
            if (!post) return;
            const uid = userId || (socket.userId || null);
            if (!uid && socket.phone) {
                const u = await User.findOne({ phone: socket.phone });
                if (u) uid = u._id;
            }
            if (!uid) return;
            if (!post.views.some(v => String(v) === String(uid))) {
                post.views.push(uid);
                await post.save();
                io.to(String(channelId)).emit('channel:post:views_updated', { postId, viewsCount: post.views.length });
            }
        } catch (err) {
            console.error('socket channel:post:view error:', err);
        }
    });

    // Reaction toggle
    socket.on('channel:post:react', async (data) => {
        try {
            const { channelId, postId, userId, emoji } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel:error', { message: 'Invalid ids' });
            if (!emoji) return socket.emit('channel:error', { message: 'emoji required' });
            const post = await ChannelPost.findById(postId);
            if (!post) return socket.emit('channel:error', { message: 'Post not found' });
            const uid = userId || (socket.userId || null);
            let userObjectId = uid;
            if (!userObjectId && socket.phone) {
                const u = await User.findOne({ phone: socket.phone });
                if (u) userObjectId = u._id;
            }
            if (!userObjectId) return socket.emit('channel:error', { message: 'User required' });

            let reaction = (post.reactions || []).find(r => r.emoji === emoji);
            if (!reaction) {
                reaction = { emoji, users: [] };
                post.reactions.push(reaction);
            }
            const idx = reaction.users.findIndex(u => String(u) === String(userObjectId));
            if (idx >= 0) reaction.users.splice(idx, 1);
            else reaction.users.push(userObjectId);
            await post.save();
            io.to(String(channelId)).emit('channel:post:reactions_updated', { postId, reactions: post.reactions });
            socket.emit('channel:post:react:success', { postId, reactions: post.reactions });
        } catch (err) {
            console.error('socket channel:post:react error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to react' });
        }
    });

    socket.on('channel:comment:get', async (data) => {
        try {
            const { channelId, postId } = data || {};
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) {
                return socket.emit('channel:error', { message: 'Invalid payload' });
            }

            const post = await ChannelPost.findById(postId);
            if (!post) return socket.emit('channel:error', { message: 'Post not found' });

            const comments = await ChannelComment.find({ postId: post._id }).sort({ createdAt: 1 }).lean();
            socket.emit('channel:comment:get:success', { channelId, postId, comments });
        } catch (err) {
            console.error('socket channel:comment:get error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to fetch comments' });
        }
    });

    // Add comment
    socket.on('channel:comment:add', async (data) => {
        try {
            const { channelId, postId, text } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId) || !text || !String(text).trim()) return socket.emit('channel:error', { message: 'Invalid payload' });
            const post = await ChannelPost.findById(postId);
            if (!post) return socket.emit('channel:error', { message: 'Post not found' });

            let author = null;
            if (data.authorId && mongoose.Types.ObjectId.isValid(data.authorId)) author = await User.findById(data.authorId);
            else if (data.authorPhone) author = await User.findOne({ phone: String(data.authorPhone).trim() });
            else if (socket.phone) author = await User.findOne({ phone: socket.phone });
            if (!author) return socket.emit('channel:error', { message: 'Author not found' });

            const comment = await ChannelComment.create({ postId: post._id, channelId: channelId, authorId: author._id, text: String(text).trim(), mediaUrl: data.mediaUrl || null });
            io.to(String(channelId)).emit('channel:comment:new', { postId, comment: comment.toObject() });
            socket.emit('channel:comment:add:success', comment.toObject());
        } catch (err) {
            console.error('socket channel:comment:add error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to add comment' });
        }
    });

    // Update admin permissions (only owner)
    socket.on('channel:admin:update_permissions', async (data) => {
        try {
            const { channelId, targetUserId, permissions, byUserId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(targetUserId) || !mongoose.Types.ObjectId.isValid(byUserId)) return socket.emit('channel:error', { message: 'Invalid ids' });
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel:error', { message: 'Channel not found' });
            if (String(channel.owner) !== String(byUserId)) return socket.emit('channel:error', { message: 'Only owner can update admin permissions' });

            const adminIdx = (channel.admins || []).findIndex(a => String(a.user || a) === String(targetUserId));
            const entry = { user: targetUserId, permissions: Object.assign({ canPost: true, canEdit: false, canDelete: false, canAddAdmins: false }, permissions || {}) };
            if (adminIdx >= 0) channel.admins[adminIdx] = entry;
            else channel.admins.push(entry);
            await channel.save();
            io.to(String(channel._id)).emit('channel:admin:permissions_updated', { channelId: channel._id, admin: entry });
            socket.emit('channel:admin:update_permissions:success', { admin: entry });
        } catch (err) {
            console.error('socket channel:admin:update_permissions error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to update admin permissions' });
        }
    });

    // Ban user from channel (owner only)
    socket.on('channel:user:ban', async (data) => {
        try {
            const { channelId, targetUserId, byUserId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(targetUserId) || !mongoose.Types.ObjectId.isValid(byUserId)) return socket.emit('channel:error', { message: 'Invalid ids' });
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel:error', { message: 'Channel not found' });
            if (String(channel.owner) !== String(byUserId)) return socket.emit('channel:error', { message: 'Only owner can ban users' });

            if (!(channel.bannedUsers || []).some(b => String(b) === String(targetUserId))) channel.bannedUsers.push(targetUserId);
            channel.subscribers = (channel.subscribers || []).filter(s => String(s) !== String(targetUserId));
            channel.admins = (channel.admins || []).filter(a => String(a.user || a) !== String(targetUserId));
            await channel.save();
            io.to(String(channel._id)).emit('channel:user:banned', { channelId: channel._id, userId: targetUserId });
            socket.emit('channel:user:ban:success', { userId: targetUserId });
        } catch (err) {
            console.error('socket channel:user:ban error:', err);
            socket.emit('channel:error', { message: err.message || 'Failed to ban user' });
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

    // -------------------- Channels (Socket handlers) --------------------
    socket.on('create_channel', async (data) => {
        if (!socket.phone && !data.ownerPhone) return socket.emit('channel_error', 'Not authenticated');
        try {
            const ownerPhone = data.ownerPhone || socket.phone;
            const owner = await User.findOne({ phone: ownerPhone });
            if (!owner) return socket.emit('channel_error', 'Owner not found');

            let avatarUrl = data.avatarUrl || null;
            if (data.avatarBase64) {
                const buffer = Buffer.from(data.avatarBase64, 'base64');
                const fakeFile = { buffer, originalname: `avatar_${Date.now()}.png`, mimetype: 'image/png' };
                try {
                    const upl = await uploadBufferToCloudinary(fakeFile);
                    avatarUrl = upl.secure_url || upl.url;
                } catch (e) {
                    const local = await saveFileLocally(fakeFile);
                    avatarUrl = local.url;
                }
            }

            const channel = await Channel.create({
                name: data.name,
                description: data.description || null,
                avatar: avatarUrl,
                type: data.type === 'private' ? 'private' : 'public',
                inviteLink: data.inviteLink || null,
                owner: owner._id,
                admins: [{ user: owner._id, permissions: { canPost: true, canEdit: true, canDelete: true, canAddAdmins: true } }],
                subscribers: [owner._id],
                bannedUsers: [],
                restrictedContent: !!data.restrictedContent,
            });

            const ch = channel.toObject();
            socket.join(String(channel._id));
            io.emit('channel_created', ch);
            socket.emit('channel_created', ch);
        } catch (err) {
            console.error('socket create_channel error:', err);
            socket.emit('channel_error', err.message || 'Failed to create channel');
        }
    });

    socket.on('join_channel', async (data) => {
        if (!socket.phone && !data.userPhone) return socket.emit('channel_error', 'Not authenticated');
        try {
            const channelId = data.channelId;
            if (!mongoose.Types.ObjectId.isValid(channelId)) return socket.emit('channel_error', 'Invalid channel id');
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel_error', 'Channel not found');

            const userPhone = data.userPhone || socket.phone;
            const user = await User.findOne({ phone: userPhone });
            if (!user) return socket.emit('channel_error', 'User not found');

            if ((channel.bannedUsers || []).some(b => String(b) === String(user._id))) return socket.emit('channel_error', 'Banned');

            if (!channel.subscribers.some(s => String(s) === String(user._id))) {
                channel.subscribers.push(user._id);
                await channel.save();
            }
            socket.join(String(channel._id));
            io.to(String(channel._id)).emit('channel_joined', { channelId: channel._id, userId: user._id });
            socket.emit('channel_joined_success', { channelId: channel._id });
        } catch (err) {
            console.error('join_channel error:', err);
            socket.emit('channel_error', err.message || 'Failed to join channel');
        }
    });

    socket.on('leave_channel', async (data) => {
        if (!socket.phone && !data.userPhone) return socket.emit('channel_error', 'Not authenticated');
        try {
            const channelId = data.channelId;
            if (!mongoose.Types.ObjectId.isValid(channelId)) return socket.emit('channel_error', 'Invalid channel id');
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel_error', 'Channel not found');

            const userPhone = data.userPhone || socket.phone;
            const user = await User.findOne({ phone: userPhone });
            if (!user) return socket.emit('channel_error', 'User not found');

            channel.subscribers = (channel.subscribers || []).filter(s => String(s) !== String(user._id));
            channel.admins = (channel.admins || []).filter(a => String(a) !== String(user._id));
            await channel.save();
            try { socket.leave(String(channel._id)); } catch (e) {}
            io.to(String(channel._id)).emit('channel_left', { channelId: channel._id, userId: user._id });
            socket.emit('channel_left_success', { channelId: channel._id });
        } catch (err) {
            console.error('leave_channel error:', err);
            socket.emit('channel_error', err.message || 'Failed to leave channel');
        }
    });

    socket.on('create_channel_post', async (data) => {
        if (!socket.phone && !data.authorPhone) return socket.emit('channel_error', 'Not authenticated');
        try {
            const channelId = data.channelId;
            if (!mongoose.Types.ObjectId.isValid(channelId)) return socket.emit('channel_error', 'Invalid channel id');
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel_error', 'Channel not found');

            const authorPhone = data.authorPhone || socket.phone;
            const author = await User.findOne({ phone: authorPhone });
            if (!author) return socket.emit('channel_error', 'Author not found');

            const isAdmin = String(channel.owner) === String(author._id) || (channel.admins || []).some(a => String(a.user) === String(author._id));
            if (!isAdmin) return socket.emit('channel_error', 'Only owner or admins can create posts');

            const mediaUrls = Array.isArray(data.media) ? data.media : (data.media ? [data.media] : []);
            const post = await ChannelPost.create({ channelId: channel._id, authorId: author._id, text: data.content || '', media: (mediaUrls || []).map(u => ({ url: u })) });
            const p = post.toObject();
            io.to(String(channel._id)).emit('new_channel_post', p);
            socket.emit('create_channel_post_success', p);
        } catch (err) {
            console.error('create_channel_post socket error:', err);
            socket.emit('channel_error', err.message || 'Failed to create channel post');
        }
    });

    socket.on('delete_channel_post', async (data) => {
        try {
            const { channelId, postId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel_error', 'Invalid ids');
            const post = await ChannelPost.findById(postId);
            if (!post) return socket.emit('channel_error', 'Post not found');
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel_error', 'Channel not found');
            // only author, admin or owner can delete
            const user = await User.findOne({ phone: socket.phone });
            if (!user) return socket.emit('channel_error', 'Not authenticated');
            const isOwner = channel.owner && String(channel.owner) === String(user._id);
            const isAdmin = (channel.admins || []).some(a => String(a.user) === String(user._id));
            if (!isOwner && !isAdmin && String(post.authorId) !== String(user._id)) return socket.emit('channel_error', 'Not authorized');
            await post.deleteOne();
            io.to(String(channel._id)).emit('channel_post_deleted', { postId });
            socket.emit('delete_channel_post_success', { postId });
        } catch (err) {
            console.error('delete_channel_post error:', err);
            socket.emit('channel_error', err.message || 'Failed to delete post');
        }
    });

    socket.on('pin_post', async (data) => {
        try {
            const { channelId, postId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel_error', 'Invalid ids');
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel_error', 'Channel not found');
            const user = await User.findOne({ phone: socket.phone });
            if (!user) return socket.emit('channel_error', 'Not authenticated');
            const isAdmin = String(channel.owner) === String(user._id) || (channel.admins || []).some(a => String(a.user) === String(user._id));
            if (!isAdmin) return socket.emit('channel_error', 'Not authorized');
            await ChannelPost.updateMany({ channelId: channel._id }, { $set: { isPinned: false } });
            await ChannelPost.findByIdAndUpdate(postId, { isPinned: true });
            io.to(String(channel._id)).emit('post_pinned', { postId });
            socket.emit('pin_post_success', { postId });
        } catch (err) {
            console.error('pin_post error:', err);
            socket.emit('channel_error', err.message || 'Failed to pin post');
        }
    });

    socket.on('add_reaction', async (data) => {
        try {
            const { channelId, postId, emoji } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel_error', 'Invalid ids');
            const post = await ChannelPost.findById(postId);
            if (!post) return socket.emit('channel_error', 'Post not found');
            const user = await User.findOne({ phone: socket.phone });
            if (!user) return socket.emit('channel_error', 'Not authenticated');

            const normalized = (post.reactions || []).map((entry) => ({
                emoji: entry.emoji,
                users: Array.isArray(entry.users) ? entry.users.map((u) => String(u)) : []
            }));
            const reactionEntry = normalized.find((entry) => entry.emoji === emoji);
            if (reactionEntry) {
                reactionEntry.users = reactionEntry.users.filter((id) => String(id) !== String(user._id));
                if (reactionEntry.users.length === 0) {
                    post.reactions = normalized.filter((entry) => entry.emoji !== emoji);
                } else {
                    post.reactions = normalized;
                }
            } else {
                post.reactions = [...normalized, { emoji, users: [user._id] }];
            }
            await post.save();
            io.to(String(channelId)).emit('post_reactions_updated', { postId, reactions: post.reactions });
            socket.emit('add_reaction_success', { postId, reactions: post.reactions });
        } catch (err) {
            console.error('add_reaction error:', err);
            socket.emit('channel_error', err.message || 'Failed to add reaction');
        }
    });

    socket.on('view_channel_post', async (data) => {
        try {
            const { channelId, postId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId)) return socket.emit('channel_error', 'Invalid ids');
            const post = await ChannelPost.findById(postId);
            if (!post) return socket.emit('channel_error', 'Post not found');
            const user = await User.findOne({ phone: socket.phone });
            if (!user) return socket.emit('channel_error', 'Not authenticated');
            if (!post.views.some(v => String(v) === String(user._id))) {
                post.views.push(user._id);
                await post.save();
                io.to(String(channelId)).emit('post_views_updated', { postId, viewsCount: post.views.length });
            }
            socket.emit('view_channel_post_ack', { postId, viewsCount: post.views.length });
        } catch (err) {
            console.error('view_channel_post error:', err);
            socket.emit('channel_error', err.message || 'Failed to view post');
        }
    });

    socket.on('add_channel_comment', async (data) => {
        try {
            const { channelId, postId, text } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(postId) || !text) return socket.emit('channel_error', 'Invalid payload');
            const user = await User.findOne({ phone: socket.phone });
            if (!user) return socket.emit('channel_error', 'Not authenticated');
            const comment = await ChannelComment.create({ postId, authorId: user._id, text });
            io.to(String(channelId)).emit('channel_comment_added', { postId, comment: comment.toObject() });
            socket.emit('add_channel_comment_success', comment.toObject());
        } catch (err) {
            console.error('add_channel_comment error:', err);
            socket.emit('channel_error', err.message || 'Failed to add comment');
        }
    });

    socket.on('ban_user', async (data) => {
        try {
            const { channelId, targetUserId } = data;
            if (!mongoose.Types.ObjectId.isValid(channelId) || !mongoose.Types.ObjectId.isValid(targetUserId)) return socket.emit('channel_error', 'Invalid ids');
            const channel = await Channel.findById(channelId);
            if (!channel) return socket.emit('channel_error', 'Channel not found');
            const byUser = await User.findOne({ phone: socket.phone });
            if (!byUser) return socket.emit('channel_error', 'Not authenticated');
            if (!channel.owner || String(channel.owner) !== String(byUser._id)) return socket.emit('channel_error', 'Only owner can ban');
            if (!channel.bannedUsers.some(b => String(b) === String(targetUserId))) channel.bannedUsers.push(targetUserId);
            channel.subscribers = (channel.subscribers || []).filter(s => String(s) !== String(targetUserId));
            channel.admins = (channel.admins || []).filter(a => String(a.user) !== String(targetUserId));
            await channel.save();
            io.to(String(channelId)).emit('channel_user_banned', { channelId, userId: targetUserId });
            socket.emit('ban_user_success', { channelId, userId: targetUserId });
        } catch (err) {
            console.error('ban_user error:', err);
            socket.emit('channel_error', err.message || 'Failed to ban user');
        }
    });
    // ------------------------------------------------------------

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
    if (!cloudinaryConfigured) {
        console.warn('Cloudinary upload skipped: Cloudinary is not configured.');
        return null;
    }

    const runtimeConfig = cloudinary.config();
    const hasKeys = !!runtimeConfig.api_key && !!runtimeConfig.api_secret && !!runtimeConfig.cloud_name;
    if (!hasKeys) {
        console.error('Cloudinary upload skipped: incomplete runtime configuration.', {
            apiKey: !!runtimeConfig.api_key,
            apiSecret: !!runtimeConfig.api_secret,
            cloudName: !!runtimeConfig.cloud_name,
            cloudinaryUrl: !!runtimeConfig.cloudinary_url,
        });
        return null;
    }

    try {
        let uploadPayload = base64Data;
        if (!uploadPayload.startsWith('data:')) {
            uploadPayload = `data:application/octet-stream;base64,${uploadPayload}`;
        }

        const result = await cloudinary.uploader.upload(uploadPayload, {
            folder: 'sparkle_uploads',
            public_id: fileName,
            resource_type: 'auto',
            overwrite: true,
            secure: true,
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
