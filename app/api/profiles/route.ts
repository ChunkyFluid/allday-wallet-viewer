import { NextRequest, NextResponse } from 'next/server';
import { pool } from '@/lib/db'; // same pool you use everywhere else

// GET /api/profiles?query=chunky
export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const query = (searchParams.get('query') || '').trim();

  if (!query) {
    return NextResponse.json({ ok: true, profiles: [] });
  }

  try {
    // This query:
    // - Starts from wallet_profiles (display_name + wallet_address)
    // - Joins wallet_holdings to get holdings + is_locked
    // - Joins nft_core_metadata / editions to get tier
    // - Aggregates per (display_name, wallet_address)
    //
    const sql = `
SELECT *
FROM public.wallet_profile_stats
WHERE LOWER(display_name) LIKE LOWER($1 || '%')
ORDER BY total_moments DESC
LIMIT 50;

    `;

    const values = [query];
    const { rows } = await pool.query(sql, values);

    return NextResponse.json({
      ok: true,
      profiles: rows,
    });
  } catch (err) {
    console.error('GET /api/profiles error:', err);
    return NextResponse.json(
      { ok: false, error: 'Failed to load profiles' },
      { status: 500 },
    );
  }
}
