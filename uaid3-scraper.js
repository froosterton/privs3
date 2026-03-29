const { Builder, By, until } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const axios = require('axios');
const express = require('express');
const { Client } = require('discord.js-selfbot-v13');

// Configuration - Environment variables for Railway
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://discord.com/api/webhooks/1486117350087462941/xhNveUHasBmQRIavL4kwaYWGdKYCoTI2thJnMIzvdZ3ranR_kbyFTF8klJiqXTlWhE_H';
const USERNAME_WEBHOOK_URL = process.env.USERNAME_WEBHOOK_URL || 'https://discord.com/api/webhooks/1424544927215259774/CIwNwKw8SSM2LIxubgMoZjGioui_3Qmoz6h9VGSTqvZL_1eRcQ-hFmaQc_KuvabCToIo';
const ITEM_IDS = process.env.ITEM_IDS || '71499623';
const NEXUS_ADMIN_KEY = process.env.NEXUS_ADMIN_KEY;
const NEXUS_API_URL = process.env.NEXUS_API_URL || 'https://discord.latticesite.com/lookup/roblox';

// Discord bot configuration
const USER_TOKEN = process.env.USER_TOKEN;
const COMMAND_CHANNEL_ID = process.env.COMMAND_CHANNEL_ID || '1465283527166922802';
const MONITOR_CHANNEL_ID = process.env.MONITOR_CHANNEL_ID || '1465281410821656618';

// Speed settings
const PAGE_LOAD_WAIT = 2000;
const TABLE_WAIT = 1500;
const PROFILE_CHECK_WAIT = 2000;
const BETWEEN_CHECKS_WAIT = 500;
const PAGES_PER_BATCH = 10;

let driver;
let processedUAIDs = new Set();
let totalFound = 0;
let isScraping = false;

// --- Nexus / Lattice Discord lookup (`data` or `payload`, `X-Access-Key`, `?query=` = Roblox username) ---

function extractDiscordFromRecord(record) {
    if (!record || typeof record !== 'object') return null;

    if (record.discord_tag) return String(record.discord_tag);
    if (record.discord_username && record.discriminator) {
        return `${record.discord_username}#${record.discriminator}`;
    }
    if (record.discord_username) return String(record.discord_username);
    if (record.global_name) return String(record.global_name);
    if (record.display_name) return String(record.display_name);
    if (record.tag && typeof record.tag === 'string') return record.tag;

    if (record.username) return String(record.username);

    const key = Object.keys(record).find(k => k.toLowerCase().includes('discord'));
    if (key && record[key]) {
        return String(record[key]);
    }

    return null;
}

function getNexusDiscordRecords(body) {
    if (!body || typeof body !== 'object') return [];
    const from = (v) => (Array.isArray(v) ? v : []);
    let records = from(body.data);
    if (!records.length) records = from(body.payload);
    if (!records.length && body.result && typeof body.result === 'object') {
        records = from(body.result.data).length ? from(body.result.data) : from(body.result.payload);
    }
    return records.filter(r => r && typeof r === 'object');
}

async function lookupDiscordAndSend(robloxUsername, rolimonsData) {
    if (!NEXUS_ADMIN_KEY) {
        console.error('❌ NEXUS_ADMIN_KEY is not set — cannot call Nexus lookup');
        return false;
    }
    try {
        const response = await axios.get(NEXUS_API_URL, {
            params: { query: robloxUsername },
            headers: {
                'X-Access-Key': NEXUS_ADMIN_KEY
            },
            validateStatus: () => true
        });

        const body = response.data;
        if (response.status !== 200) {
            const detail = body && (body.detail ?? body.message);
            console.error(`❌ Nexus API HTTP ${response.status} for ${robloxUsername}:`, detail || body || '(no body)');
            return false;
        }

        if (body && body.ok === false && body.error_msg) {
            console.log(`ℹ️ Nexus ok=false for ${robloxUsername}: ${body.error_msg}`);
        }

        let records = getNexusDiscordRecords(body);
        records = [...records].sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0));

        const discordRecord = records.find(r => extractDiscordFromRecord(r));
        const discordValue = discordRecord ? extractDiscordFromRecord(discordRecord) : null;

        if (!discordValue) {
            const keys = body && typeof body === 'object' ? Object.keys(body) : [];
            console.log(`ℹ️ No Discord found for ${robloxUsername} (Nexus: empty or unparseable records; top-level keys: ${keys.join(', ') || 'n/a'})`);
            return false;
        }

        await sendToWebhook(robloxUsername, discordValue, discordRecord, rolimonsData);
        await sendUsernameOnlyToWebhook(discordValue);
        return true;
    } catch (error) {
        const data = error.response && error.response.data;
        console.error(`❌ Nexus API error for ${robloxUsername}:`, error.message, data ? JSON.stringify(data).slice(0, 300) : '');
        return false;
    }
}

async function sendToWebhook(robloxUsername, discordUsername, discordRecord, rolimonsData) {
    console.log(`📤 sendToWebhook called: Roblox=${robloxUsername}, Discord=${discordUsername}`);
    try {
        const fields = [];

        fields.push({
            name: 'Discord Username',
            value: discordUsername,
            inline: false
        });

        if (discordRecord && discordRecord.user_id) {
            fields.push({
                name: 'Discord ID',
                value: discordRecord.user_id.toString(),
                inline: true
            });
        } else if (discordRecord && discordRecord.id) {
            fields.push({
                name: 'Discord ID',
                value: discordRecord.id.toString(),
                inline: true
            });
        }

        fields.push({
            name: 'Roblox Username',
            value: robloxUsername,
            inline: true
        });

        if (rolimonsData && rolimonsData.value) {
            fields.push({
                name: 'Value',
                value: rolimonsData.value.toLocaleString(),
                inline: true
            });
        }

        if (rolimonsData && rolimonsData.tradeAds !== undefined) {
            fields.push({
                name: 'Trade Ads',
                value: rolimonsData.tradeAds.toString(),
                inline: true
            });
        }

        const embed = {
            title: '✨ New Discord Found!',
            color: 0x00ae86,
            fields,
            timestamp: new Date().toISOString()
        };

        if (rolimonsData && rolimonsData.avatarUrl) {
            embed.thumbnail = { url: rolimonsData.avatarUrl };
        }

        if (rolimonsData && rolimonsData.profileUrl) {
            fields.push({
                name: 'Rolimons Profile',
                value: `[View Profile](${rolimonsData.profileUrl})`,
                inline: false
            });
        }

        const payload = { embeds: [embed] };

        console.log('Sending webhook: new Discord found...');
        const response = await axios.post(WEBHOOK_URL, payload);
        console.log('✅ Webhook sent successfully, status:', response.status);
    } catch (e) {
        console.error('❌ Webhook POST error:', e.message);
        if (e.response) {
            console.error('Response status:', e.response.status);
            console.error('Response data:', e.response.data);
        }
    }
}

async function sendUsernameOnlyToWebhook(discordUsername) {
    console.log(`📤 Sending Discord username only to username webhook: ${discordUsername}`);
    try {
        const payload = { content: discordUsername };
        const response = await axios.post(USERNAME_WEBHOOK_URL, payload);
        console.log('✅ Username-only webhook sent successfully, status:', response.status);
    } catch (e) {
        console.error('❌ Username-only webhook POST error:', e.message);
        if (e.response) {
            console.error('Response status:', e.response.status);
            console.error('Response data:', e.response.data);
        }
    }
}

// Fetch channel history via Discord REST (user token — same as selfbot)
async function fetchAllMessages(channelId, startMessageId = null, endMessageId = null) {
    const usernames = [];
    let lastMessageId = startMessageId || null;
    let foundStartMessage = !startMessageId;
    let foundEndMessage = false;
    const botToken = USER_TOKEN;

    if (!botToken) {
        console.error('❌ USER_TOKEN not set, cannot fetch messages from Discord API');
        return [];
    }

    console.log('📥 Fetching messages from Discord API...');

    while (true) {
        try {
            const url = `https://discord.com/api/v10/channels/${channelId}/messages`;
            const params = { limit: 100 };
            if (lastMessageId) params.before = lastMessageId;

            const response = await axios.get(url, {
                headers: {
                    Authorization: botToken,
                    'Content-Type': 'application/json'
                },
                params
            });

            const messages = response.data;
            if (!messages || messages.length === 0) break;

            for (const msg of messages) {
                if (endMessageId && msg.id === endMessageId) {
                    foundEndMessage = true;
                    break;
                }

                if (startMessageId && !foundStartMessage) {
                    if (msg.id === startMessageId) foundStartMessage = true;
                    else continue;
                }

                if (foundStartMessage && !foundEndMessage) {
                    if (msg.webhook_id) {
                        if (msg.embeds && msg.embeds.length > 0) {
                            for (const embed of msg.embeds) {
                                if (embed.fields) {
                                    for (const field of embed.fields) {
                                        if (field.name === 'Discord Username' && field.value) {
                                            const username = field.value.trim();
                                            if (username && !usernames.includes(username)) usernames.push(username);
                                        }
                                    }
                                }
                            }
                        }
                        if (msg.content && msg.content.trim()) {
                            const content = msg.content.trim();
                            if (!content.includes(' ') && content.length > 0 && !usernames.includes(content)) {
                                usernames.push(content);
                            }
                        }
                    }
                }
                lastMessageId = msg.id;
            }

            if (foundEndMessage) break;
            if (messages.length < 100) break;
            await new Promise(resolve => setTimeout(resolve, 500));
        } catch (error) {
            console.error('❌ Error fetching messages:', error.message);
            if (error.response) {
                console.error('Response status:', error.response.status);
                console.error('Response data:', error.response.data);
            }
            break;
        }
    }

    return usernames.reverse();
}

async function findMessageIdByUsername(channelId, username) {
    const botToken = USER_TOKEN;
    if (!botToken) {
        console.error('❌ USER_TOKEN not set, cannot search for messages');
        return null;
    }

    let lastMessageId = null;
    console.log(`🔍 Searching for message with username: ${username}`);

    while (true) {
        try {
            const url = `https://discord.com/api/v10/channels/${channelId}/messages`;
            const params = { limit: 100 };
            if (lastMessageId) params.before = lastMessageId;

            const response = await axios.get(url, {
                headers: {
                    Authorization: botToken,
                    'Content-Type': 'application/json'
                },
                params
            });

            const messages = response.data;
            if (!messages || messages.length === 0) break;

            for (const msg of messages) {
                if (msg.webhook_id) {
                    if (msg.embeds && msg.embeds.length > 0) {
                        for (const embed of msg.embeds) {
                            if (embed.fields) {
                                for (const field of embed.fields) {
                                    if (field.name === 'Discord Username' && field.value && field.value.trim() === username) {
                                        return msg.id;
                                    }
                                }
                            }
                        }
                    }
                    if (msg.content && msg.content.trim() === username) return msg.id;
                }
                lastMessageId = msg.id;
            }

            if (messages.length < 100) break;
            await new Promise(resolve => setTimeout(resolve, 500));
        } catch (error) {
            console.error('❌ Error searching for message:', error.message);
            break;
        }
    }

    return null;
}

// Express server for Railway health check
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
    res.json({
        status: 'healthy',
        scraping: isScraping,
        totalFound: totalFound,
        processedUAIDs: processedUAIDs.size,
        timestamp: new Date().toISOString()
    });
});

app.listen(PORT, () => {
    console.log(`🌐 Health check server running on port ${PORT}`);
    console.log('ℹ️  This process stays alive for Railway health checks.');
    if (!USER_TOKEN) {
        console.log('⚠️  USER_TOKEN not set — Discord commands (!total, !makefile, …) will not work.');
    }
    if (!NEXUS_ADMIN_KEY) {
        console.log('⚠️  NEXUS_ADMIN_KEY not set — lookupDiscordAndSend will fail until you set it.');
    }
    console.log('ℹ️  There is no UAID/Selenium scraper entry in this file: nothing polls Rolimons until you add/run that loop (e.g. from scrapper.js or restore main()).');
});

// Initialize Discord client for commands
let discordClient = null;
if (USER_TOKEN) {
    discordClient = new Client({ checkUpdate: false });

    discordClient.on('ready', () => {
        console.log(`✅ Discord bot logged in as ${discordClient.user.tag}`);
        console.log(`👀 Accepting commands from channel ${COMMAND_CHANNEL_ID}`);
        console.log(`📥 Reading usernames from channel ${MONITOR_CHANNEL_ID}`);
    });

    discordClient.on('messageCreate', async (message) => {
        // Only listen to commands from the command channel
        if (message.channel.id !== COMMAND_CHANNEL_ID) return;
        if (!message.content.startsWith('!')) return;
        
        const command = message.content.trim();
        
        // Command: !total
        if (command === '!total') {
            console.log('📊 Processing !total command...');
            await message.reply('🔄 Fetching all usernames from channel history... This may take a moment.');
            
            try {
                const usernames = await fetchAllMessages(MONITOR_CHANNEL_ID);
                const count = usernames.length;
                
                if (count === 0) {
                    await message.reply('❌ No Discord usernames found in channel history.');
                } else {
                    await message.reply(`✅ Found **${count}** unique Discord username(s) in channel history.\n\nUse \`!makefiletotal\` to export the full list to a file.`);
                }
            } catch (error) {
                console.error('❌ Error processing !total:', error.message);
                await message.reply(`❌ Error: ${error.message}`);
            }
        }
        
        // Command: !totalfrom (username) to (username)
        else if (command.startsWith('!totalfrom')) {
            console.log('📊 Processing !totalfrom command...');
            const match = command.match(/^!totalfrom\s+(.+?)\s+to\s+(.+)$/);
            
            if (!match) {
                await message.reply('❌ Invalid format. Use: `!totalfrom <username> to <username>`');
                return;
            }
            
            const startUsername = match[1].trim();
            const endUsername = match[2].trim();
            
            await message.reply(`🔄 Searching for messages between "${startUsername}" and "${endUsername}"...`);
            
            try {
                const startMessageId = await findMessageIdByUsername(MONITOR_CHANNEL_ID, startUsername);
                const endMessageId = await findMessageIdByUsername(MONITOR_CHANNEL_ID, endUsername);
                
                if (!startMessageId) {
                    await message.reply(`❌ Could not find message with username: ${startUsername}`);
                    return;
                }
                if (!endMessageId) {
                    await message.reply(`❌ Could not find message with username: ${endUsername}`);
                    return;
                }
                
                const usernames = await fetchAllMessages(MONITOR_CHANNEL_ID, startMessageId, endMessageId);
                const count = usernames.length;
                
                if (count === 0) {
                    await message.reply('❌ No Discord usernames found between the specified messages.');
                } else {
                    await message.reply(`✅ Found **${count}** unique Discord username(s) between messages.\n\nUse \`!makefile ${startUsername} to ${endUsername}\` to export the full list to a file.`);
                }
            } catch (error) {
                console.error('❌ Error processing !totalfrom:', error.message);
                await message.reply(`❌ Error: ${error.message}`);
            }
        }
        
        // Command: !makefiletotal
        else if (command === '!makefiletotal') {
            console.log('📝 Processing !makefiletotal command...');
            await message.reply('🔄 Creating file with all usernames... This may take a moment.');
            
            try {
                const usernames = await fetchAllMessages(MONITOR_CHANNEL_ID);
                const count = usernames.length;
                
                if (count === 0) {
                    await message.reply('❌ No Discord usernames found. File not created.');
                    return;
                }
                
                const filename = `discord_usernames_total_${Date.now()}.txt`;
                const content = usernames.join('\n');
                
                await message.reply({
                    content: `✅ **${filename}**\n📊 Contains **${count}** Discord username(s).\n📎 File attached below.`,
                    files: [{ attachment: Buffer.from(content, 'utf8'), name: filename }]
                });
            } catch (error) {
                console.error('❌ Error processing !makefiletotal:', error.message);
                await message.reply(`❌ Error: ${error.message}`);
            }
        }
        
        // Command: !makefile (username) to (username)
        else if (command.startsWith('!makefile')) {
            console.log('📝 Processing !makefile command...');
            const match = command.match(/^!makefile\s+(.+?)\s+to\s+(.+)$/);
            
            if (!match) {
                await message.reply('❌ Invalid format. Use: `!makefile <username> to <username>`');
                return;
            }
            
            const startUsername = match[1].trim();
            const endUsername = match[2].trim();
            
            await message.reply(`🔄 Creating file with usernames between "${startUsername}" and "${endUsername}"...`);
            
            try {
                const startMessageId = await findMessageIdByUsername(MONITOR_CHANNEL_ID, startUsername);
                const endMessageId = await findMessageIdByUsername(MONITOR_CHANNEL_ID, endUsername);
                
                if (!startMessageId) {
                    await message.reply(`❌ Could not find message with username: ${startUsername}`);
                    return;
                }
                if (!endMessageId) {
                    await message.reply(`❌ Could not find message with username: ${endUsername}`);
                    return;
                }
                
                const usernames = await fetchAllMessages(MONITOR_CHANNEL_ID, startMessageId, endMessageId);
                const count = usernames.length;
                
                if (count === 0) {
                    await message.reply('❌ No Discord usernames found. File not created.');
                    return;
                }
                
                const filename = `discord_usernames_${startUsername}_to_${endUsername}_${Date.now()}.txt`;
                const content = usernames.join('\n');
                
                await message.reply({
                    content: `✅ **${filename}**\n📊 Contains **${count}** Discord username(s).\n📎 File attached below.`,
                    files: [{ attachment: Buffer.from(content, 'utf8'), name: filename }]
                });
            } catch (error) {
                console.error('❌ Error processing !makefile:', error.message);
                await message.reply(`❌ Error: ${error.message}`);
            }
        }
    });

    discordClient.on('error', (e) => console.error('❌ Discord client error:', e));

    discordClient.login(USER_TOKEN).catch((e) => {
        console.error('❌ Failed to login to Discord (check USER_TOKEN):', e.message || e);
    });
} else {
    console.log('⚠️ USER_TOKEN not set — Discord command features disabled');
}
