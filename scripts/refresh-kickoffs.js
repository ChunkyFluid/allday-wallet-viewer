// scripts/refresh-kickoffs.js
// Admin script to fetch kickoff data from NFL All Day using Playwright
// Run: node scripts/refresh-kickoffs.js

import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, '..', 'data');
const KICKOFFS_FILE = path.join(DATA_DIR, 'kickoffs.json');
const CHALLENGES_FILE = path.join(DATA_DIR, 'challenges.json');
const AUTH_FILE = path.join(DATA_DIR, 'nflad-auth.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

async function fetchWithRetry(page, body, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const result = await page.evaluate(async (bodyStr) => {
        const res = await fetch('https://nflallday.com/consumer/graphql', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
          credentials: 'same-origin',
          body: bodyStr
        });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        return res.json();
      }, JSON.stringify(body));
      return result;
    } catch (err) {
      console.error(`  Attempt ${i + 1} failed:`, err.message);
      if (i < retries - 1) {
        await new Promise(r => setTimeout(r, 2000));
      }
    }
  }
  return null;
}

async function fetchKickoffSlates(page) {
  console.log('Fetching kickoff slates...');
  
  const body = {
    operationName: 'SearchKickoffSlates',
    variables: {
      input: {
        after: '',
        first: 100,
        filters: {},
        sortBy: 'START_DATE_DESC'
      }
    },
    query: `query SearchKickoffSlates($input: SearchKickoffSlatesInput!) {
      searchKickoffSlates(input: $input) {
        edges {
          node {
            id
            name
            startDate
            endDate
            status
            kickoffs {
              id
              name
              slateID
              difficulty
              status
              submissionDeadline
              gamesStartAt
              completedAt
              numParticipants
              slots {
                id
                slotOrder
                stats {
                  id
                  stat
                  valueNeeded
                  valueType
                  groupV2
                }
                requirements {
                  playerPositions
                  tiers
                  badgeSlugs
                  setIDs
                  teamIDs
                }
              }
            }
          }
          cursor
        }
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }`
  };

  const result = await fetchWithRetry(page, body);
  if (!result?.data?.searchKickoffSlates) {
    console.error('Failed to fetch kickoff slates');
    return null;
  }

  return result.data.searchKickoffSlates;
}

async function fetchChallenges(page) {
  console.log('Fetching challenges...');
  
  const body = {
    operationName: 'SearchChallenges',
    variables: {
      input: {
        after: '',
        first: 50,
        filters: {}
      }
    },
    query: `query SearchChallenges($input: SearchChallengesInput!) {
      searchChallenges(input: $input) {
        edges {
          node {
            id
            slug
            title
            subtitle
            description
            category
            status
            startsAt
            endsAt
            completedAt
            rewardsDescription
            submissions {
              totalCount
            }
            requirements {
              description
              count
              filters {
                byTiers
                byPlayerPositions
                bySetIDs
                byTeamIDs
                byPlayerIDs
              }
            }
          }
          cursor
        }
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }`
  };

  const result = await fetchWithRetry(page, body);
  if (!result?.data?.searchChallenges) {
    console.error('Failed to fetch challenges');
    return null;
  }

  return result.data.searchChallenges;
}

async function main() {
  console.log('='.repeat(60));
  console.log('NFL All Day Data Refresh');
  console.log('='.repeat(60));
  console.log('');

  let browser;

  try {
    // Always launch with visible browser so user can log in if needed
    console.log('Launching browser (visible)...');
    browser = await chromium.launch({ 
      headless: false,
      args: ['--disable-blink-features=AutomationControlled']
    });

    // Create context with saved auth if available
    let context;
    if (fs.existsSync(AUTH_FILE)) {
      console.log('Loading saved authentication...');
      try {
        const storageState = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8'));
        context = await browser.newContext({ storageState });
      } catch (e) {
        console.warn('Could not load saved auth, starting fresh');
        context = await browser.newContext();
      }
    } else {
      context = await browser.newContext();
    }

    const page = await context.newPage();

    // Navigate to NFL All Day
    console.log('Navigating to NFL All Day...');
    await page.goto('https://nflallday.com/games', { 
      waitUntil: 'domcontentloaded',
      timeout: 60000 
    });

    // Wait a bit for any JS to load
    await page.waitForTimeout(3000);

    // Check if we need to login
    const isLoggedIn = await page.evaluate(() => {
      // Check for login indicators
      const loginBtn = document.querySelector('[data-testid="login-button"]');
      const signInText = document.body.innerText.includes('Sign In');
      return !loginBtn && !signInText;
    });

    if (!isLoggedIn) {
      console.log('');
      console.log('='.repeat(60));
      console.log('LOGIN REQUIRED');
      console.log('='.repeat(60));
      console.log('Please log in to NFL All Day in the browser window.');
      console.log('After logging in, the script will continue automatically.');
      console.log('');
      
      // Wait for user to log in (look for changes in the page)
      console.log('Waiting for login (max 5 minutes)...');
      try {
        await page.waitForFunction(() => {
          // Check for indicators that user is logged in
          const loginBtn = document.querySelector('[data-testid="login-button"]');
          return !loginBtn;
        }, { timeout: 300000 });
        
        console.log('Login detected! Saving authentication...');
        await page.waitForTimeout(2000);
        
      } catch (e) {
        console.error('Login timeout or error:', e.message);
        console.log('Please run the script again after logging in.');
        await browser.close();
        process.exit(1);
      }
    } else {
      console.log('Already logged in, continuing...');
    }

    // Fetch data (whether we just logged in or were already logged in)
    console.log('');
    console.log('Fetching data...');
    
    const slatesData = await fetchKickoffSlates(page);
    const challengesData = await fetchChallenges(page);
    
    await saveData(slatesData, challengesData);
    
    // Save auth for next time
    console.log('Saving authentication for next time...');
    await context.storageState({ path: AUTH_FILE });

    await browser.close();
    
  } catch (err) {
    console.error('Error:', err);
    if (browser) await browser.close();
    process.exit(1);
  }
}

async function saveData(slatesData, challengesData) {
  const timestamp = new Date().toISOString();
  
  if (slatesData) {
    const kickoffsOutput = {
      lastUpdated: timestamp,
      totalSlates: slatesData.totalCount,
      slates: slatesData.edges.map(e => e.node)
    };
    
    // Also flatten kickoffs for easy access
    const allKickoffs = [];
    for (const slate of kickoffsOutput.slates) {
      if (slate.kickoffs) {
        for (const kickoff of slate.kickoffs) {
          allKickoffs.push({
            ...kickoff,
            slateName: slate.name,
            slateStatus: slate.status,
            slateStartDate: slate.startDate,
            slateEndDate: slate.endDate
          });
        }
      }
    }
    kickoffsOutput.kickoffs = allKickoffs;
    kickoffsOutput.totalKickoffs = allKickoffs.length;
    
    fs.writeFileSync(KICKOFFS_FILE, JSON.stringify(kickoffsOutput, null, 2));
    console.log(`✓ Saved ${kickoffsOutput.totalKickoffs} kickoffs to ${KICKOFFS_FILE}`);
  }
  
  if (challengesData) {
    const challengesOutput = {
      lastUpdated: timestamp,
      totalChallenges: challengesData.totalCount,
      challenges: challengesData.edges.map(e => e.node)
    };
    
    fs.writeFileSync(CHALLENGES_FILE, JSON.stringify(challengesOutput, null, 2));
    console.log(`✓ Saved ${challengesOutput.totalChallenges} challenges to ${CHALLENGES_FILE}`);
  }
  
  console.log('');
  console.log('='.repeat(60));
  console.log('Data refresh complete!');
  console.log(`Timestamp: ${timestamp}`);
  console.log('='.repeat(60));
}

main();
