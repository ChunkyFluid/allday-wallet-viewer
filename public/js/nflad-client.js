/**
 * NFL All Day Client-Side GraphQL Fetcher
 * 
 * Makes GraphQL requests directly from the browser to bypass server-side 403 blocks.
 * Your browser session/cookies are automatically included, authenticating the request.
 */

const NFLAD_GRAPHQL_URL = 'https://nflallday.com/consumer/graphql';

// Cache for client-side requests
const clientCache = new Map();
const CLIENT_CACHE_TTL = 2 * 60 * 1000; // 2 minutes

/**
 * Make a GraphQL request directly from the browser
 */
async function nfladQuery(queryName, query, variables = {}) {
  const cacheKey = JSON.stringify({ queryName, variables });
  
  // Check cache
  const cached = clientCache.get(cacheKey);
  if (cached && Date.now() < cached.expiresAt) {
    console.log(`[NFLAD Client] Cache hit for ${queryName}`);
    return { ...cached.data, cached: true };
  }
  
  console.log(`[NFLAD Client] Fetching ${queryName}...`);
  
  try {
    const response = await fetch(`${NFLAD_GRAPHQL_URL}?${queryName}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      credentials: 'include', // Include cookies!
      body: JSON.stringify({ query, variables })
    });
    
    if (!response.ok) {
      const text = await response.text();
      console.error(`[NFLAD Client] HTTP ${response.status}:`, text.substring(0, 200));
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const data = await response.json();
    
    if (data.errors) {
      console.error(`[NFLAD Client] GraphQL errors:`, data.errors);
      throw new Error(data.errors[0]?.message || 'GraphQL error');
    }
    
    // Cache successful response
    clientCache.set(cacheKey, {
      data: data.data,
      expiresAt: Date.now() + CLIENT_CACHE_TTL
    });
    
    console.log(`[NFLAD Client] Success for ${queryName}`);
    return { ...data.data, cached: false };
    
  } catch (err) {
    console.error(`[NFLAD Client] Error for ${queryName}:`, err);
    throw err;
  }
}

/**
 * Fetch active kickoffs (Playbook)
 */
async function fetchKickoffs(statuses = ['STARTED', 'NOT_STARTED', 'GAMES_IN_PROGRESS']) {
  const query = `
    query searchKickoffs($input: SearchKickoffsInput!) {
      searchKickoffs(input: $input) {
        edges {
          node {
            id
            name
            slateID
            difficulty
            status
            submissionDeadline
            gamesStartAt
            completedAt
            numParticipants
          }
        }
        totalCount
      }
    }
  `;
  
  const variables = {
    input: {
      first: 30,
      filters: { byStatuses: statuses }
    }
  };
  
  const data = await nfladQuery('searchKickoffs', query, variables);
  return {
    kickoffs: data.searchKickoffs?.edges?.map(e => e.node) || [],
    totalCount: data.searchKickoffs?.totalCount || 0,
    cached: data.cached
  };
}

/**
 * Fetch challenges
 */
async function fetchChallenges(statuses = ['ACTIVE', 'UPCOMING']) {
  const query = `
    query searchChallenges($input: SearchChallengesInput!) {
      searchChallenges(input: $input) {
        edges {
          node {
            id
            name
            description
            status
            startDate
            endDate
            reward {
              id
              name
              description
              type
            }
            requirements {
              id
              description
              type
              count
            }
          }
        }
        totalCount
      }
    }
  `;
  
  const variables = {
    input: {
      first: 30,
      filters: { byStatuses: statuses }
    }
  };
  
  const data = await nfladQuery('searchChallenges', query, variables);
  return {
    challenges: data.searchChallenges?.edges?.map(e => e.node) || [],
    totalCount: data.searchChallenges?.totalCount || 0,
    cached: data.cached
  };
}

/**
 * Fetch trade-in leaderboards
 */
async function fetchTradeInLeaderboards() {
  const query = `
    query GetPublishedLeaderboards {
      getPublishedLeaderboards {
        id
        slug
        name
        description
        startDate
        endDate
        status
        totalEntries
      }
    }
  `;
  
  const data = await nfladQuery('GetPublishedLeaderboards', query, {});
  return {
    leaderboards: data.getPublishedLeaderboards || [],
    cached: data.cached
  };
}

/**
 * Fetch specific trade-in leaderboard entries
 */
async function fetchLeaderboardEntries(idOrSlug, first = 50) {
  const query = `
    query GetLeaderboard($input: GetLeaderboardInput!, $first: Int) {
      getLeaderboard(input: $input) {
        id
        slug
        name
        description
        status
        totalEntries
        entries(first: $first) {
          edges {
            node {
              rank
              score
              user {
                displayName
                username
              }
              submittedAt
            }
          }
          totalCount
        }
      }
    }
  `;
  
  // Try slug first
  let variables = { input: { slug: idOrSlug }, first };
  
  try {
    const data = await nfladQuery('GetLeaderboard', query, variables);
    return {
      leaderboard: data.getLeaderboard,
      entries: data.getLeaderboard?.entries?.edges?.map(e => e.node) || [],
      cached: data.cached
    };
  } catch (err) {
    // Try by ID if slug fails
    variables = { input: { id: idOrSlug }, first };
    const data = await nfladQuery('GetLeaderboard', query, variables);
    return {
      leaderboard: data.getLeaderboard,
      entries: data.getLeaderboard?.entries?.edges?.map(e => e.node) || [],
      cached: data.cached
    };
  }
}

/**
 * Fetch user's kickoff submissions (requires being logged in)
 */
async function fetchMySubmissions(kickoffId) {
  const query = `
    query searchKickoffSubmissions($input: SearchKickoffSubmissionsInput!) {
      searchKickoffSubmissions(input: $input) {
        edges {
          node {
            id
            kickoffID
            slotID
            momentNFT {
              id
              serialNumber
              flowID
              player {
                fullName
              }
            }
            totalPoints
          }
        }
        totalCount
      }
    }
  `;
  
  const variables = {
    input: {
      first: 50,
      filters: { byKickoffIDs: [kickoffId] }
    }
  };
  
  const data = await nfladQuery('searchKickoffSubmissions', query, variables);
  return {
    submissions: data.searchKickoffSubmissions?.edges?.map(e => e.node) || [],
    totalCount: data.searchKickoffSubmissions?.totalCount || 0,
    cached: data.cached
  };
}

// Export for use in pages
window.NFLADClient = {
  query: nfladQuery,
  fetchKickoffs,
  fetchChallenges,
  fetchTradeInLeaderboards,
  fetchLeaderboardEntries,
  fetchMySubmissions,
  clearCache: () => clientCache.clear()
};

console.log('[NFLAD Client] Loaded - use window.NFLADClient to make requests');

