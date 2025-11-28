'use client';

import { useState, useEffect } from 'react';

type ProfileRow = {
  display_name: string;
  wallet_address: string;
  total_moments: number;
  unlocked_moments: number;
  locked_moments: number;
  tier_common: number;
  tier_rare: number;
  tier_legendary: number;
  tier_ultimate: number;
};

export default function ProfilesPage() {
  const [query, setQuery] = useState('');
  const [profiles, setProfiles] = useState<ProfileRow[]>([]);
  const [selected, setSelected] = useState<ProfileRow | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch profiles when query changes with a small debounce
  useEffect(() => {
    if (!query.trim()) {
      setProfiles([]);
      setSelected(null);
      return;
    }

    const handle = setTimeout(async () => {
      setLoading(true);
      setError(null);

      try {
        const res = await fetch(`/api/profiles?query=${encodeURIComponent(query.trim())}`);
        const data = await res.json();

        if (!res.ok || !data.ok) {
          throw new Error(data.error || 'Failed to load profiles');
        }

        setProfiles(data.profiles || []);

        // Auto-select first profile if none selected
        if (!selected && data.profiles && data.profiles.length > 0) {
          setSelected(data.profiles[0]);
        } else if (data.profiles.length === 0) {
          setSelected(null);
        }
      } catch (err: any) {
        console.error(err);
        setError(err.message || 'Something went wrong');
      } finally {
        setLoading(false);
      }
    }, 350);

    return () => clearTimeout(handle);
  }, [query]);

  const totalTiers = (p: ProfileRow | null) => {
    if (!p) return 0;
    return (
      (p.tier_common || 0) +
      (p.tier_rare || 0) +
      (p.tier_legendary || 0) +
      (p.tier_ultimate || 0)
    );
  };

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-950 via-slate-900 to-slate-950 text-slate-100 px-6 py-8">
      <div className="max-w-6xl mx-auto">
        <header className="mb-8">
          <h1 className="text-2xl font-semibold tracking-tight">Matching profiles</h1>
          <p className="text-sm text-slate-400 mt-1">
            Enter a Dapper display name to see matching wallets.
          </p>
        </header>

        {/* Search */}
        <div className="mb-8">
          <label className="block text-xs font-medium text-slate-400 mb-1">
            Dapper display name
          </label>
          <div className="flex gap-3">
            <input
              type="text"
              placeholder="e.g. Chunky, CoolUser, etc."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="w-full rounded-xl bg-slate-900/70 border border-slate-700 px-3 py-2 text-sm outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
            />
          </div>
          <p className="mt-2 text-xs text-slate-500">
            Shows wallets whose display name starts with what you type.
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-[ minmax(0,2fr)_minmax(0,3fr) ] gap-6">
          {/* Left: list of matching wallets */}
          <section className="bg-slate-950/60 border border-slate-800 rounded-2xl p-4 flex flex-col">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-semibold text-slate-200">Wallets</h2>
              {loading && (
                <span className="text-xs text-slate-500 animate-pulse">
                  Loading…
                </span>
              )}
            </div>

            {error && (
              <div className="mb-3 text-xs text-red-400 bg-red-900/20 border border-red-900/50 rounded-lg px-3 py-2">
                {error}
              </div>
            )}

            {profiles.length === 0 && !loading && query.trim() && (
              <p className="text-sm text-slate-500">
                No wallets found for <span className="font-semibold">{query}</span>.
              </p>
            )}

            {profiles.length === 0 && !query.trim() && (
              <p className="text-sm text-slate-500">
                Start typing a display name above to see matching wallets.
              </p>
            )}

            {profiles.length > 0 && (
              <ul className="mt-1 space-y-1 overflow-auto max-h-[420px] pr-1">
                {profiles.map((profile) => {
                  const isSelected =
                    selected?.wallet_address === profile.wallet_address &&
                    selected?.display_name === profile.display_name;

                  return (
                    <li key={profile.wallet_address} className="">
                      <button
                        type="button"
                        onClick={() => setSelected(profile)}
                        className={`w-full text-left rounded-xl px-3 py-2 text-xs border transition
                          ${
                            isSelected
                              ? 'bg-indigo-600/20 border-indigo-500/80'
                              : 'bg-slate-950/40 border-slate-800 hover:bg-slate-900/60 hover:border-slate-600'
                          }
                        `}
                      >
                        <div className="flex items-center justify-between gap-2">
                          <div>
                            <div className="font-semibold text-slate-100 truncate">
                              {profile.display_name}
                            </div>
                            <div className="text-[10px] text-slate-400 truncate">
                              {profile.wallet_address}
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-[11px] text-slate-200">
                              {profile.total_moments.toLocaleString()} moments
                            </div>
                            <div className="text-[10px] text-emerald-400">
                              {profile.unlocked_moments.toLocaleString()} unlocked
                            </div>
                          </div>
                        </div>
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </section>

          {/* Right: snapshot for selected wallet */}
          <section className="bg-slate-950/60 border border-slate-800 rounded-2xl p-4 flex flex-col">
            <h2 className="text-sm font-semibold text-slate-200 mb-1">
              Wallet snapshot
            </h2>
            <p className="text-xs text-slate-400 mb-4">
              Select a wallet to see total moments, locked vs unlocked, and tier breakdown.
            </p>

            {!selected && (
              <div className="flex-1 flex items-center justify-center text-sm text-slate-500">
                No wallet selected.
              </div>
            )}

            {selected && (
              <div className="space-y-4">
                {/* Header */}
                <div className="rounded-xl bg-slate-900/80 border border-slate-700 px-4 py-3">
                  <div className="text-xs text-slate-400">Display name</div>
                  <div className="text-sm font-semibold text-slate-100">
                    {selected.display_name}
                  </div>
                  <div className="mt-2 text-[11px] text-slate-400 break-all">
                    {selected.wallet_address}
                  </div>
                </div>

                {/* Totals */}
                <div className="grid grid-cols-3 gap-3 text-center">
                  <div className="rounded-xl bg-slate-900/70 border border-slate-700 px-2 py-3">
                    <div className="text-[11px] text-slate-400 mb-1">Total</div>
                    <div className="text-lg font-semibold">
                      {selected.total_moments.toLocaleString()}
                    </div>
                  </div>
                  <div className="rounded-xl bg-slate-900/70 border border-slate-700 px-2 py-3">
                    <div className="text-[11px] text-slate-400 mb-1">Unlocked</div>
                    <div className="text-lg font-semibold text-emerald-400">
                      {selected.unlocked_moments.toLocaleString()}
                    </div>
                  </div>
                  <div className="rounded-xl bg-slate-900/70 border border-slate-700 px-2 py-3">
                    <div className="text-[11px] text-slate-400 mb-1">Locked</div>
                    <div className="text-lg font-semibold text-amber-400">
                      {selected.locked_moments.toLocaleString()}
                    </div>
                  </div>
                </div>

                {/* Tier breakdown */}
                <div className="rounded-xl bg-slate-900/80 border border-slate-700 px-4 py-3">
                  <div className="flex items-center justify-between mb-3">
                    <div>
                      <div className="text-xs font-semibold text-slate-200">
                        Tier breakdown
                      </div>
                      <div className="text-[11px] text-slate-500">
                        {totalTiers(selected)} tracked
                      </div>
                    </div>
                  </div>

                  <dl className="space-y-1 text-xs">
                    <TierRow label="Common" value={selected.tier_common} />
                    <TierRow label="Rare" value={selected.tier_rare} />
                    <TierRow label="Legendary" value={selected.tier_legendary} />
                    <TierRow label="Ultimate" value={selected.tier_ultimate} />
                  </dl>
                </div>

                {/* CTA to jump to wallet page */}
                <div className="pt-2">
                  <a
                    href={`/wallet?address=${encodeURIComponent(
                      selected.wallet_address,
                    )}`}
                    className="inline-flex items-center justify-center rounded-xl border border-indigo-500 bg-indigo-600/80 px-4 py-2 text-xs font-semibold text-white hover:bg-indigo-500 transition"
                  >
                    View full wallet
                  </a>
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

function TierRow({ label, value }: { label: string; value: number }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-slate-300">{label}</dt>
      <dd className="text-slate-100 font-semibold">
        {value != null ? value.toLocaleString() : '0'}
      </dd>
    </div>
  );
}
