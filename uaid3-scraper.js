const { Builder, By, until } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const axios = require('axios');
const express = require('express');

// Configuration — set WEBHOOK_URL, USERNAME_WEBHOOK_URL, and NEXUS_ADMIN_KEY in the environment (e.g. Railway).
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const USERNAME_WEBHOOK_URL = process.env.USERNAME_WEBHOOK_URL;
const ITEM_IDS = process.env.ITEM_IDS || '215718515';
const NEXUS_ADMIN_KEY = process.env.NEXUS_ADMIN_KEY;
const NEXUS_API_URL = process.env.NEXUS_API_URL || 'https://discord.latticesite.com/lookup/roblox';
// Default 800 when unset; set ALL_COPIES_START_PAGE= (empty) on Railway to scrape from the last page down.
const ALL_COPIES_START_PAGE = process.env.ALL_COPIES_START_PAGE ?? '70';

// Chrome runs headless (--headless=new) by default for Railway/server use.
// All Copies: ALL_COPIES_START_PAGE (1-based) = highest page to include; default 800 unless env overrides.

// Speed settings
const PAGE_LOAD_WAIT = 2000;
const TABLE_WAIT = 1500;
const PROFILE_CHECK_WAIT = 2000;
const PAGES_PER_BATCH = 10;
/** Delay after each Prev click while stepping between page numbers (may round up slightly in Node/WebDriver). */
const PREV_PAGINATION_DELAY_MS = 0.01;

let driver;
let processedUAIDs = new Set();
let totalFound = 0;
let isScraping = false;
/** Set when Rolimons shows the “loading pages too quickly” page; scrape stops after one webhook ping. */
let scrapeAbortedRateLimit = false;

const ROLIMONS_RATE_LIMIT_SNIPPET = 'loading pages too quickly';

function pageSourceIsRolimonsRateLimited(html) {
    return typeof html === 'string' && html.includes(ROLIMONS_RATE_LIMIT_SNIPPET);
}

async function notifyWebhookRateLimitedStop() {
    if (scrapeAbortedRateLimit) return;
    scrapeAbortedRateLimit = true;
    console.error('🛑 Rolimons rate limit page detected; alerting webhook and ending scrape.');
    if (!WEBHOOK_URL) {
        console.error('⚠️ WEBHOOK_URL not set; cannot send rate-limit alert.');
        return;
    }
    try {
        await axios.post(WEBHOOK_URL, {
            content: '@everyone',
            allowed_mentions: { parse: ['everyone'] },
            embeds: [
                {
                    title: '⚠️ Rolimons rate limit',
                    description: 'End scrape, Rate limited!',
                    color: 0xe74c3c,
                    timestamp: new Date().toISOString()
                }
            ]
        });
        console.log('✅ Rate-limit alert sent to Discord webhook.');
    } catch (e) {
        console.error('❌ Failed to send rate-limit webhook:', e.message);
    }
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
});

/**
 * 1-based highest page to scrape (inclusive). Pages above this are skipped.
 * Unset/empty = scrape from last detected page through page 1.
 */
function resolveAllCopiesStartPage(envValue, totalPages) {
    if (envValue == null || String(envValue).trim() === '') {
        return totalPages;
    }
    const n = parseInt(String(envValue).trim(), 10);
    if (Number.isNaN(n) || n < 1) {
        console.log(`⚠️ ALL_COPIES_START_PAGE invalid (${envValue}); using full range (page ${totalPages} → 1).`);
        return totalPages;
    }
    if (n > totalPages) {
        console.log(`⚠️ ALL_COPIES_START_PAGE (${n}) > detected pages (${totalPages}); using last page as start.`);
        return totalPages;
    }
    return n;
}

async function initializeWebDriver() {
    try {
        console.log('🔧 Initializing Selenium WebDriver...');
        console.log('🖥️ Chrome: headless');

        const options = new chrome.Options();
        options.addArguments('--headless=new');
        options.addArguments('--no-sandbox');
        options.addArguments('--disable-dev-shm-usage');
        options.addArguments('--disable-gpu');
        options.addArguments('--window-size=1920,1080');
        options.addArguments('--disable-web-security');
        options.addArguments('--disable-features=VizDisplayCompositor');
        options.addArguments('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        options.addArguments('--disable-blink-features=AutomationControlled');

        driver = await new Builder()
            .forBrowser('chrome')
            .setChromeOptions(options)
            .build();

        console.log('✅ Selenium WebDriver initialized');
        return true;
    } catch (error) {
        console.error('❌ WebDriver initialization error:', error.message);
        return false;
    }
}

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

    // Nexus /lookup/roblox: { "username": "<discord username>", "score": ..., "server_ids": [...] }
    if (record.username) return String(record.username);

    const key = Object.keys(record).find(k => k.toLowerCase().includes('discord'));
    if (key && record[key]) {
        return String(record[key]);
    }

    return null;
}

/** Read candidate rows from `data`, `payload`, or nested `result`. */
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

async function lookupDiscordUsername(robloxUsername) {
    if (!NEXUS_ADMIN_KEY) {
        console.log(`  ⚠️ NEXUS_ADMIN_KEY not set, skipping Discord lookup`);
        return null;
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
            console.error(`  ❌ Nexus API HTTP ${response.status} for ${robloxUsername}:`, detail || body || '(no body)');
            return null;
        }

        if (body && body.ok === false && body.error_msg) {
            console.log(`  ℹ️ Nexus ok=false for ${robloxUsername}: ${body.error_msg}`);
        }

        let records = getNexusDiscordRecords(body);
        records = [...records].sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0));

        const discordRecord = records.find(r => extractDiscordFromRecord(r));
        const discordValue = discordRecord ? extractDiscordFromRecord(discordRecord) : null;

        if (!discordValue) {
            console.log(`  ℹ️ No Discord found for ${robloxUsername}`);
            return null;
        }

        console.log(`  🎮 Discord found: ${discordValue}`);
        return discordValue;
    } catch (error) {
        const data = error.response && error.response.data;
        console.error(`  ❌ Nexus API error for ${robloxUsername}:`, error.message, data ? JSON.stringify(data).slice(0, 300) : '');
        return null;
    }
}

async function sendToWebhook(userData) {
    if (!WEBHOOK_URL) {
        console.error('❌ WEBHOOK_URL not set; cannot send embed.');
        return false;
    }
    console.log(`📤 Sending embed to webhook: ${userData.username}`);
    try {
        const embed = {
            title: "✨ New Profile Found!",
            color: 0x00AE86,
            fields: [
                {
                    name: "Discord Username",
                    value: userData.discord || " ",
                    inline: false
                },
                {
                    name: "Roblox Username",
                    value: userData.username,
                    inline: true
                },
                {
                    name: "Rolimons Profile",
                    value: `[View Profile](${userData.profileUrl})`,
                    inline: false
                }
            ],
            timestamp: new Date().toISOString()
        };

        // Add avatar thumbnail if available
        if (userData.avatarUrl) {
            embed.thumbnail = { url: userData.avatarUrl };
        }

        const payload = { embeds: [embed] };

        const response = await axios.post(WEBHOOK_URL, payload);
        console.log('✅ Webhook sent successfully, status:', response.status);

        // Send Discord username only to the username webhook (if Discord was found)
        if (userData.discord) {
            await sendUsernameToWebhook(userData.discord);
        }

        return true;
    } catch (e) {
        console.error('❌ Webhook POST error:', e.message);
        if (e.response) {
            console.error('Response status:', e.response.status);
        }
        return false;
    }
}

async function sendUsernameToWebhook(discordUsername) {
    if (!USERNAME_WEBHOOK_URL) {
        console.error('❌ USERNAME_WEBHOOK_URL not set; skipping username-only webhook.');
        return false;
    }
    try {
        const payload = { content: discordUsername };
        const response = await axios.post(USERNAME_WEBHOOK_URL, payload);
        console.log('✅ Username webhook sent, status:', response.status);
        return true;
    } catch (e) {
        console.error('❌ Username webhook error:', e.message);
        return false;
    }
}

async function checkUserHasAvatar(profileUrl) {
    try {
        await driver.get(profileUrl);
        await driver.sleep(PROFILE_CHECK_WAIT);

        const rateCheckSrc = await driver.getPageSource();
        if (pageSourceIsRolimonsRateLimited(rateCheckSrc)) {
            await notifyWebhookRateLimitedStop();
            return { valid: false, avatarUrl: null };
        }

        // Try to find the avatar image element specifically
        // Rolimons uses img with class containing avatar or specific container
        const avatarSelectors = [
            'img.mx-auto.d-block.w-100.h-100',
            'img[src*="rbxcdn.com"]',
            '.player-avatar img',
            '#player_avatar img'
        ];

        for (const selector of avatarSelectors) {
            try {
                const avatarImg = await driver.findElement(By.css(selector));
                const src = await avatarImg.getAttribute('src');

                if (src) {
                    // Check if it's the terminated placeholder
                    if (src.includes('transparent-square') || src.includes('placeholder')) {
                        console.log(`  ❌ TERMINATED (placeholder avatar)`);
                        return { valid: false, avatarUrl: null };
                    }

                    // Check if it's a valid rbxcdn avatar
                    if (src.includes('rbxcdn.com')) {
                        console.log(`  ✅ Valid: ${src.substring(0, 50)}...`);
                        return { valid: true, avatarUrl: src };
                    }
                }
            } catch (e) {
                // Selector not found, try next
                continue;
            }
        }

        // Fallback: check page source for avatar patterns
        const pageSource = await driver.getPageSource();

        // Look for valid rbxcdn avatar URL first (prioritize finding valid)
        const avatarMatch = pageSource.match(/https:\/\/tr\.rbxcdn\.com\/[^"'\s]+Avatar[^"'\s]*/i);
        if (avatarMatch) {
            const avatarUrl = avatarMatch[0];
            console.log(`  ✅ Valid (source): ${avatarUrl.substring(0, 50)}...`);
            return { valid: true, avatarUrl: avatarUrl };
        }

        // Check for any rbxcdn image
        const rbxcdnMatch = pageSource.match(/https:\/\/tr\.rbxcdn\.com\/[^"'\s]+/i);
        if (rbxcdnMatch) {
            const avatarUrl = rbxcdnMatch[0];
            console.log(`  ✅ Valid (rbxcdn): ${avatarUrl.substring(0, 50)}...`);
            return { valid: true, avatarUrl: avatarUrl };
        }

        // Only mark as terminated if we explicitly find the placeholder AND no valid avatar
        // Check if terminated placeholder exists in a specific context
        if (pageSource.includes('transparent-square-110.png') && !pageSource.includes('tr.rbxcdn.com')) {
            console.log(`  ❌ TERMINATED (no valid avatar found)`);
            return { valid: false, avatarUrl: null };
        }

        // Default: assume valid if we can't determine
        console.log(`  ⚠️ Could not determine avatar status, assuming valid`);
        return { valid: true, avatarUrl: null };
    } catch (error) {
        console.error('  ⚠️ Error checking avatar:', error.message);
        return { valid: true, avatarUrl: null };
    }
}

async function findPreviousOwnerFromUAID(uaidUrl) {
    try {
        await driver.get(uaidUrl);
        await driver.sleep(PAGE_LOAD_WAIT);

        const uaidHtml = await driver.getPageSource();
        if (pageSourceIsRolimonsRateLimited(uaidHtml)) {
            await notifyWebhookRateLimitedStop();
            return null;
        }

        // Find the FIRST valid player link (most recent previous owner)
        let firstOwner = null;

        try {
            const playerLinks = await driver.findElements(By.css('a[href*="/player/"]'));

            for (const link of playerLinks) {
                try {
                    const href = await link.getAttribute('href');
                    const text = await link.getText();

                    if (!text || !text.trim()) continue;
                    if (text.includes('Deleted') || text.includes('Hidden')) continue;

                    const username = text.trim();
                    let profileUrl = href;
                    if (!profileUrl.startsWith('http')) {
                        profileUrl = `https://www.rolimons.com${href}`;
                    }

                    // Found the first user - stop looking
                    firstOwner = { username, profileUrl };
                    break;
                } catch (e) {
                    continue;
                }
            }
        } catch (e) {}

        // If no user found at all, return null
        if (!firstOwner) {
            console.log(`  ⚠️ No previous owner found on UAID page`);
            return null;
        }

        console.log(`  👤 First previous owner: ${firstOwner.username}`);

        // Check ONLY the first owner - if terminated, skip this UAID entirely
        const avatarCheck = await checkUserHasAvatar(firstOwner.profileUrl);
        if (scrapeAbortedRateLimit) {
            return null;
        }

        if (avatarCheck.valid) {
            return {
                username: firstOwner.username,
                profileUrl: firstOwner.profileUrl,
                avatarUrl: avatarCheck.avatarUrl,
                discord: ""
            };
        } else {
            // First owner is terminated - skip this UAID
            console.log(`  ⏭️ First owner ${firstOwner.username} is terminated, skipping UAID`);
            return null;
        }
    } catch (error) {
        return null;
    }
}

async function navigateToItemPage(url) {
    await driver.get(url);
    await driver.sleep(PAGE_LOAD_WAIT);

    // Click "All Copies" tab
    const allCopiesTab = await driver.findElement(By.css('a[href="#all_copies_table_container"]'));
    await driver.executeScript('arguments[0].click();', allCopiesTab);
    await driver.sleep(TABLE_WAIT);
    await driver.wait(until.elementLocated(By.css('#all_copies_table tbody tr')), 15000);
}

async function navigateToPage(targetPage, totalPages) {
    // Click to specific page - first try direct click, then use pagination
    try {
        const pageBtn = await driver.findElement(By.xpath(`//a[contains(@class, 'page-link') and text()='${targetPage}']`));
        await driver.executeScript('arguments[0].click();', pageBtn);
        await driver.sleep(TABLE_WAIT);
        return true;
    } catch (e) {
        // Page button not visible, need to navigate using next/prev
        // Go to last page first, then work backwards
        try {
            const lastPageBtn = await driver.findElement(By.xpath(`//a[contains(@class, 'page-link') and text()='${totalPages}']`));
            await driver.executeScript('arguments[0].click();', lastPageBtn);
            await driver.sleep(TABLE_WAIT);

            // Click prev until we reach target
            for (let p = totalPages; p > targetPage; p--) {
                const prevLink = await driver.findElement(By.css('#all_copies_table_paginate a.page-link[data-dt-idx="0"]'));
                await driver.executeScript('arguments[0].click();', prevLink);
                await driver.sleep(PREV_PAGINATION_DELAY_MS);
            }
            return true;
        } catch (e2) {
            return false;
        }
    }
}

async function collectUAIDsFromCurrentPage() {
    let uaids = [];
    const rows = await driver.findElements(By.css('#all_copies_table tbody tr'));

    for (let i = rows.length - 1; i >= 0; i--) {
        try {
            const row = rows[i];

            // Check if deleted/hidden (no player link)
            let hasPlayerLink = false;
            try {
                const playerLink = await row.findElement(By.css('a[href*="/player/"]'));
                const playerText = await playerLink.getText();
                if (playerText && playerText.trim() && !playerText.includes('Deleted') && !playerText.includes('Hidden')) {
                    hasPlayerLink = true;
                }
            } catch (e) {
                hasPlayerLink = false;
            }

            if (!hasPlayerLink) {
                try {
                    const uaidElement = await row.findElement(By.css('a[href*="/uaid/"]'));
                    const uaidHref = await uaidElement.getAttribute('href');
                    const uaidText = await uaidElement.getText();

                    let uaidUrl = uaidHref;
                    if (!uaidUrl.startsWith('http')) {
                        uaidUrl = `https://www.rolimons.com${uaidHref}`;
                    }

                    if (!processedUAIDs.has(uaidText)) {
                        uaids.push({ uaid: uaidText, url: uaidUrl });
                        processedUAIDs.add(uaidText);
                    }
                } catch (e) {}
            }
        } catch (e) {}
    }

    return uaids;
}

async function scrapeItemForDeletedUsers(itemId) {
    try {
        const url = `https://www.rolimons.com/item/${itemId}`;
        console.log(`\n🔍 Scraping item: ${url}`);

        await driver.get(url);
        await driver.sleep(PAGE_LOAD_WAIT);

        // Get item name
        let itemName = 'Unknown Item';
        try {
            const titleElement = await driver.findElement(By.css('h1.page_title.mb-0'));
            itemName = await titleElement.getText();
            console.log(`📦 Item: ${itemName}`);
        } catch (e) {}

        // Click "All Copies" tab
        try {
            const allCopiesTab = await driver.findElement(By.css('a[href="#all_copies_table_container"]'));
            await driver.executeScript('arguments[0].click();', allCopiesTab);
            await driver.sleep(TABLE_WAIT);
            await driver.wait(until.elementLocated(By.css('#all_copies_table tbody tr')), 15000);
        } catch (e) {
            console.log('❌ Could not load All Copies table:', e.message);
            return;
        }

        // Find total pages
        let totalPages = 1;
        try {
            await driver.wait(until.elementLocated(By.css('#all_copies_table_paginate')), 10000);
            const pageButtons = await driver.findElements(By.css('#all_copies_table_paginate a.page-link[data-dt-idx]'));

            for (const button of pageButtons) {
                const text = (await button.getText()).trim();
                if (/^\d+$/.test(text)) {
                    const pageNum = parseInt(text, 10);
                    if (pageNum > totalPages) totalPages = pageNum;
                }
            }
        } catch (e) {}

        const startPage = resolveAllCopiesStartPage(ALL_COPIES_START_PAGE, totalPages);
        if (startPage < totalPages) {
            console.log(`📌 ALL_COPIES_START_PAGE=${startPage}: skipping pages above ${startPage} (detected ${totalPages} total).`);
        }
        console.log(`📄 Scraping pages ${startPage} → 1 in batches of ${PAGES_PER_BATCH}`);

        // Process in batches of PAGES_PER_BATCH pages (from startPage downward)
        let currentPage = startPage;
        let batchNum = 0;

        while (currentPage >= 1) {
            if (scrapeAbortedRateLimit) {
                console.log('🛑 Stopping item scrape (rate limited).');
                return;
            }

            batchNum++;
            const batchEnd = currentPage;
            const batchStart = Math.max(1, currentPage - PAGES_PER_BATCH + 1);

            console.log(`\n📦 Batch ${batchNum}: Pages ${batchEnd} → ${batchStart} (of ${totalPages} total)`);

            // Navigate to starting page of this batch
            await navigateToItemPage(url);
            if (batchEnd > 1) {
                await navigateToPage(batchEnd, totalPages);
            }

            // Collect UAIDs from this batch of pages
            let batchUAIDs = [];

            for (let page = batchEnd; page >= batchStart; page--) {
                if (page !== batchEnd) {
                    try {
                        const prevLink = await driver.findElement(By.css('#all_copies_table_paginate a.page-link[data-dt-idx="0"]'));
                        await driver.executeScript('arguments[0].click();', prevLink);
                        await driver.sleep(PREV_PAGINATION_DELAY_MS);
                    } catch (e) {
                        break;
                    }
                }

                const pageUAIDs = await collectUAIDsFromCurrentPage();
                batchUAIDs.push(...pageUAIDs);
                console.log(`  Page ${page}: ${pageUAIDs.length} UAIDs (batch total: ${batchUAIDs.length})`);
            }

            // Process this batch
            if (batchUAIDs.length > 0) {
                console.log(`\n⚡ Processing ${batchUAIDs.length} UAIDs from batch ${batchNum}...`);

                for (let i = 0; i < batchUAIDs.length; i++) {
                    if (scrapeAbortedRateLimit) break;

                    const { uaid, url: uaidUrl } = batchUAIDs[i];
                    console.log(`[${i + 1}/${batchUAIDs.length}] UAID: ${uaid}`);

                    const userData = await findPreviousOwnerFromUAID(uaidUrl);
                    if (scrapeAbortedRateLimit) break;

                    if (userData) {
                        console.log(`  ✨ Found: ${userData.username}`);

                        // Lookup Discord username via Nexus API
                        const discordUsername = await lookupDiscordUsername(userData.username);

                        // Only send embed if Discord was found
                        if (discordUsername) {
                            userData.discord = discordUsername;
                            await sendToWebhook(userData);
                            totalFound++;
                        } else {
                            console.log(`  ⏭️ Skipping ${userData.username} - no Discord found`);
                        }
                    } else {
                        console.log(`  ❌ No valid owner`);
                    }
                }
            } else {
                console.log(`  No deleted/hidden users in this batch`);
            }

            if (scrapeAbortedRateLimit) {
                console.log('🛑 Stopping item scrape (rate limited).');
                return;
            }

            // Move to next batch
            currentPage = batchStart - 1;
        }

        if (scrapeAbortedRateLimit) {
            return;
        }

        console.log(`\n✅ Finished item ${itemId}. Total found: ${totalFound}`);
    } catch (error) {
        console.error('❌ Error scraping item:', error.message);
    }
}

async function main() {
    console.log('🚀 UAID Previous Owner Scraper');
    console.log('================================');
    console.log('This script finds Deleted/Hidden users and looks up their previous owners.\n');

    // Check configurations
    if (NEXUS_ADMIN_KEY) {
        console.log('✅ Nexus API configured - Discord lookups enabled');
    } else {
        console.log('⚠️ NEXUS_ADMIN_KEY not set - Discord lookups disabled');
    }
    console.log('');

    const initialized = await initializeWebDriver();
    if (!initialized) {
        console.error('❌ Failed to initialize WebDriver');
        process.exit(1);
    }

    const itemIds = ITEM_IDS.split(',').map(id => id.trim()).filter(id => id && !isNaN(id));
    console.log(`📋 Will scrape ${itemIds.length} items: ${itemIds.join(', ')}\n`);

    isScraping = true;

    for (const itemId of itemIds) {
        if (scrapeAbortedRateLimit) break;
        await scrapeItemForDeletedUsers(itemId);
    }

    isScraping = false;
    console.log('\n================================');
    if (scrapeAbortedRateLimit) {
        console.log(`🛑 Scrape ended early (rate limited). Total previous owners found: ${totalFound}`);
    } else {
        console.log(`🏁 All done! Total previous owners found: ${totalFound}`);
    }

    await driver.quit();
    console.log('✅ Scraping complete. Server still running for health checks.');
}

// Handle cleanup
process.on('SIGINT', async () => {
    console.log('\n🧹 Cleaning up...');
    if (driver) {
        try {
            await driver.quit();
        } catch (e) {}
    }
    process.exit(0);
});

main();
