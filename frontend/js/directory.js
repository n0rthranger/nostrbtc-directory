(function() {
    'use strict';

    var API_BASE = location.origin;
    var grid = document.getElementById('dir-grid');
    var countEl = document.getElementById('dir-count');
    var emptyEl = document.getElementById('dir-empty');
    var sentinelEl = document.getElementById('dir-sentinel');

    // State
    var currentPage = 1;
    var totalPages = 1;
    var totalMembers = 0;
    var isLoading = false;
    var allLoaded = false;
    var loadedMembers = [];
    var renderedMemberCount = 0;
    var MAX_RENDERED_MEMBERS = 500;
    window.__dirLoadedMembers = loadedMembers;

    // Observer npub for personalized trust view (free for everyone)
    var observerNpub = localStorage.getItem('directory_npub') || null;
    var currentHops = 3;

    // Cluster state
    var clusterData = null;  // {clusters: [...], assignments: {pubkey: id}}
    var activeCluster = null; // null = show all, number = filter

    // Badge definitions
    var BADGE_DEFS = {
        'relay-subscriber':    { icon: '<svg viewBox="0 0 24 24" width="14" height="14"><defs><linearGradient id="rsg" x1="0%" y1="0%" x2="100%" y2="0%"><stop offset="0%" stop-color="#8b5cf6"/><stop offset="50%" stop-color="#f7931a"/><stop offset="100%" stop-color="#8b5cf6"/></linearGradient></defs><circle cx="12" cy="12" r="3" fill="url(#rsg)"/><circle cx="12" cy="12" r="6" fill="none" stroke="url(#rsg)" stroke-width="1.5" opacity="0.8"/><circle cx="12" cy="12" r="9.5" fill="none" stroke="url(#rsg)" stroke-width="1.3" opacity="0.4"/></svg>', label: 'Relay Subscriber', color: '#f7931a', cssClass: 'dir-badge-relay' },
        'nip05-live':          { icon: '<svg viewBox="0 0 24 24" width="14" height="14"><defs><linearGradient id="dvg" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="#8b5cf6"/><stop offset="100%" stop-color="#f7931a"/></linearGradient></defs><path d="M22.25 12c0-1.43-.88-2.67-2.19-3.34.46-1.39.2-2.9-.81-3.91s-2.52-1.27-3.91-.81C14.67 2.63 13.43 1.75 12 1.75s-2.67.88-3.34 2.19c-1.39-.46-2.9-.2-3.91.81s-1.27 2.52-.81 3.91C2.63 9.33 1.75 10.57 1.75 12s.88 2.67 2.19 3.34c-.46 1.39-.2 2.9.81 3.91s2.52 1.27 3.91.81c.67 1.31 1.91 2.19 3.34 2.19s2.67-.88 3.34-2.19c1.39.46 2.9.2 3.91-.81s1.27-2.52.81-3.91c1.31-.67 2.19-1.91 2.19-3.34z" fill="url(#dvg)"/><path d="M10 16.4l-3.7-3.7 1.4-1.4 2.3 2.3 4.3-4.3 1.4 1.4L10 16.4z" fill="#fff"/></svg>', label: 'NIP-05', color: '#10b981', cssClass: 'dir-badge-verified' },
        'lightning-reachable': { icon: '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="#eab308" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L4.5 14H12l-1 8L19.5 10H12l1-8z"/></svg>', label: 'Lightning', color: '#eab308', cssClass: 'dir-badge-lightning' },
    };

    // Trust tier definitions
    var TIER_DEFS = {
        highly_trusted: { label: 'Highly Trusted', color: '#10b981', abbr: 'HT' },
        trusted:        { label: 'Trusted',        color: '#8b5cf6', abbr: 'T' },
        neutral:        { label: 'Neutral',        color: '#eab308', abbr: 'N' },
        low_trust:      { label: 'Low Trust',      color: '#f97316', abbr: 'LT' },
        unverified:     { label: 'Unverified',     color: '#6b7280', abbr: 'U' },
    };

    function escHtml(str) {
        if (!str) return '';
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // Get state from terminal
    function getTermState() {
        if (window.__dirTerminal) return window.__dirTerminal.getState();
        return { search: '', tags: [], sort: 'trust', badge: '', view: 'list' };
    }

    // -----------------------------------------------------------------------
    // Paid member UI enhancements
    // -----------------------------------------------------------------------
    function setupPaidUI() {
        // Expose __dirSetHops so observer controls can trigger re-fetches.
        window.__dirSetHops = function(n) {
            currentHops = n;
            fetchDirectory(false);
        };
    }

    var trustTabAdded = false;
    function addTrustSortTab() {
        if (trustTabAdded) return;
        var tabContainer = document.getElementById('dfilter-sort');
        if (!tabContainer) return;
        var trustTab = document.createElement('button');
        trustTab.type = 'button';
        trustTab.className = 'dfilter-sort-btn';
        trustTab.dataset.sort = 'trust';
        trustTab.innerHTML = '<svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-1px;margin-right:3px"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>Most Trusted';
        tabContainer.appendChild(trustTab);
        trustTab.addEventListener('click', function() {
            if (window.__dirTerminal && window.__dirTerminal.setSort) {
                window.__dirTerminal.setSort('trust');
            }
            syncSortTabs();
        });
        trustTabAdded = true;
    }

    // -----------------------------------------------------------------------
    // View
    // -----------------------------------------------------------------------
    function applyView() {
        var s = getTermState();
        grid.classList.remove('compact', 'dir-table-view');
        if (s.view === 'grid') {
            // card grid
        } else {
            // default: table/list view
            grid.classList.add('dir-table-view');
        }
    }

    // -----------------------------------------------------------------------
    // Stats header
    // -----------------------------------------------------------------------
    function fetchStats() {
        var p = window.__dirPrefetch && window.__dirPrefetch.stats
            ? window.__dirPrefetch.stats
            : fetch(API_BASE + '/api/directory/stats').then(function(r) {
                if (!r.ok) throw new Error('Failed to load directory stats');
                return r.json();
            });
        p.then(function(data) {
            if (!data) return;
            _animateNumber('stat-members', data.total_members || 0);
            _animateNumber('stat-active', data.active_this_week || 0);
            _animateNumber('stat-events', data.total_events || 0);
        }).catch(function(err) {
            console.warn('Directory stats unavailable:', err && err.message ? err.message : err);
        });
    }

    // -----------------------------------------------------------------------
    // Trust Clusters
    // -----------------------------------------------------------------------
    var clusterBarEl = document.getElementById('dir-clusters');

    function fetchClusters() {
        var p = window.__dirPrefetch && window.__dirPrefetch.clusters
            ? window.__dirPrefetch.clusters
            : fetch(API_BASE + '/api/directory/clusters').then(function(r) {
                if (!r.ok) throw new Error('Failed to load directory clusters');
                return r.json();
            });
        p.then(function(data) {
            if (!data || !data.clusters || data.clusters.length < 1) return;
            clusterData = data;
            renderClusterBar();
        }).catch(function(err) {
            console.warn('Directory clusters unavailable:', err && err.message ? err.message : err);
        });
    }

    function renderClusterBar() {
        if (!clusterBarEl || !clusterData || !clusterData.clusters.length) return;
        var html = '<span class="dir-cluster-label">Clusters</span>';
        html += '<button type="button" class="dir-cluster-pill' + (activeCluster === null ? ' active' : '') + '" data-cluster="all">All</button>';
        for (var i = 0; i < clusterData.clusters.length; i++) {
            var c = clusterData.clusters[i];
            var isActive = activeCluster === c.id;
            html += '<button type="button" class="dir-cluster-pill' + (isActive ? ' active' : '') + '" data-cluster="' + c.id + '" style="--cluster-color:' + escHtml(c.color) + '">' +
                '<span class="dir-cluster-dot" style="background:' + escHtml(c.color) + '"></span>' +
                escHtml(c.label) + ' <span class="dir-cluster-count">' + c.member_count + '</span>' +
            '</button>';
        }
        clusterBarEl.innerHTML = html;
        clusterBarEl.style.display = '';

        // Click handlers
        var pills = clusterBarEl.querySelectorAll('.dir-cluster-pill');
        for (var j = 0; j < pills.length; j++) {
            pills[j].addEventListener('click', function() {
                var val = this.dataset.cluster;
                activeCluster = val === 'all' ? null : parseInt(val, 10);
                renderClusterBar();
                fetchDirectory(false);
            });
        }
    }

    function getClusterColor(pubkey) {
        if (!clusterData || !clusterData.assignments) return null;
        var cid = clusterData.assignments[pubkey];
        if (cid === undefined || cid === null || cid < 0) return null;
        for (var i = 0; i < clusterData.clusters.length; i++) {
            if (clusterData.clusters[i].id === cid) return clusterData.clusters[i].color;
        }
        return null;
    }

    function getClusterLabel(pubkey) {
        if (!clusterData || !clusterData.assignments) return null;
        var cid = clusterData.assignments[pubkey];
        if (cid === undefined || cid === null || cid < 0) return null;
        for (var i = 0; i < clusterData.clusters.length; i++) {
            if (clusterData.clusters[i].id === cid) return clusterData.clusters[i].label;
        }
        return null;
    }

    function _animateNumber(id, target) {
        var el = document.getElementById(id);
        if (!el) return;
        if (target === 0) { el.textContent = '0'; return; }
        var duration = 600;
        var startTime = null;
        function step(ts) {
            if (!startTime) startTime = ts;
            var progress = Math.min((ts - startTime) / duration, 1);
            var eased = 1 - Math.pow(1 - progress, 3);
            el.textContent = _formatNumber(Math.floor(eased * target));
            if (progress < 1) requestAnimationFrame(step);
        }
        requestAnimationFrame(step);
    }

    function _formatNumber(n) {
        if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
        if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
        return String(n);
    }

    // -----------------------------------------------------------------------
    // Avatar
    // -----------------------------------------------------------------------
    function gradientAvatar(pubkey) {
        var colors = ['#f7931a','#8b5cf6','#06b6d4','#10b981','#f43f5e','#eab308','#6366f1','#ec4899'];
        var h = 0;
        for (var i = 0; i < 8; i++) h = ((h << 5) - h + pubkey.charCodeAt(i)) | 0;
        var c1 = colors[Math.abs(h) % colors.length];
        var c2 = colors[Math.abs(h >> 4) % colors.length];
        return 'linear-gradient(135deg, ' + c1 + ', ' + c2 + ')';
    }

    function makeAvatarHtml(m, size) {
        var hasImg = m.picture && m.picture.startsWith('http');
        var initial = (m.name || '?')[0].toUpperCase();
        var grad = gradientAvatar(m.pubkey);
        var cls = size === 'sm' ? 'dir-avatar-sm' : 'dir-avatar';
        var fbCls = size === 'sm' ? 'dir-avatar-fb-sm' : 'dir-avatar-fallback';
        if (hasImg) {
            return '<img class="' + cls + '" src="' + escHtml(m.picture) + '" alt="" loading="lazy" data-fallback>' +
                '<div class="' + fbCls + '" style="display:none;background:' + grad + '">' + initial + '</div>';
        }
        return '<div class="' + fbCls + '" style="background:' + grad + '">' + initial + '</div>';
    }

    // -----------------------------------------------------------------------
    // Trust tier rendering
    // -----------------------------------------------------------------------
    function trustTierHtml(m, mode) {
        var tierKey = m.trust_tier || 'unverified';
        var tier = TIER_DEFS[tierKey] || TIER_DEFS.unverified;
        var hasScore = m.trust_score !== null && m.trust_score !== undefined;
        var grapeScore = hasScore ? Math.round(Number(m.trust_score || 0) * 100) : 0;
        var repScore = Math.round(Number(m.reputation_score || 0));
        var title = 'GrapeRank ' + grapeScore + ', reputation ' + repScore;
        if (mode === 'row') {
            return '<div class="dir-row-trust" title="' + escHtml(title) + '" style="color:' + tier.color + '">' +
                '<span>GR ' + grapeScore + '</span><span class="dir-trust-score">R ' + repScore + '</span>' +
            '</div>';
        }
        return '<div class="dir-card-trust" title="' + escHtml(title) + '" style="border-color:' + tier.color + '55;color:' + tier.color + '">' +
            '<span class="dir-trust-dot" style="background:' + tier.color + '"></span>' +
            '<span>GrapeRank ' + grapeScore + '</span><span class="dir-trust-score">R ' + repScore + '</span>' +
        '</div>';
    }

    // -----------------------------------------------------------------------
    // Table row rendering (default — scales to thousands)
    // -----------------------------------------------------------------------
    function renderRow(m, rank) {
        var username = m.nip05_display || m.npub.slice(0, 16) + '...';
        var badgeDots = (m.badges || []).map(function(b) {
            var def = BADGE_DEFS[b];
            if (!def) return '';
            return '<span class="dir-row-badge" style="color:' + def.color + '" title="' + def.label + '">' + def.icon + '</span>';
        }).join('');

        var trustHtml = trustTierHtml(m, 'row');

        var rowStatusHtml = '';
        if (m.last_active > 0) {
            var rdiff = Math.floor(Date.now() / 1000) - m.last_active;
            var rClass = rdiff < 900 ? 'online' : rdiff < 3600 ? 'recent' : 'offline';
            var rText = rdiff < 900 ? 'online' : _timeAgo(m.last_active);
            rowStatusHtml = '<div class="dir-row-status ' + rClass + '"><span class="dir-status-dot"></span><span>' + rText + '</span></div>';
        }

        var safeUrl = m.card_url && m.card_url.charAt(0) === '/' ? escHtml(m.card_url) : '#';

        var rowTierClass = m.trust_tier ? ' tier-' + m.trust_tier : '';
        return '<a href="' + safeUrl + '" class="dir-row' + rowTierClass + '" data-pubkey="' + escHtml(m.pubkey) + '">' +
            '<div class="dir-row-rank">' + rank + '</div>' +
            '<div class="dir-row-avatar">' + makeAvatarHtml(m, 'sm') + '</div>' +
            '<div class="dir-row-identity">' +
                '<span class="dir-row-name">' + escHtml(m.name || 'Anonymous') + '</span>' +
                '<span class="dir-row-nip05">' + escHtml(username) + '</span>' +
            '</div>' +
            '<div class="dir-row-badges">' + badgeDots + '</div>' +
            trustHtml +
            rowStatusHtml +
        '</a>';
    }

    // -----------------------------------------------------------------------
    // Card rendering (grid view)
    // -----------------------------------------------------------------------
    function renderCard(m) {
        var username = m.nip05_display || m.npub.slice(0, 20) + '...';

        var badgeItems = (m.badges || []).map(function(b) {
            var def = BADGE_DEFS[b];
            if (!def) return '';
            var cls = 'dir-card-badge' + (def.cssClass ? ' ' + def.cssClass : '');
            return '<div class="' + cls + '">' +
                '<div class="dir-badge-icon" style="color:' + def.color + '">' + def.icon + '</div>' +
                '<div class="dir-badge-text"><span class="dir-badge-label">' + def.label + '</span></div>' +
            '</div>';
        }).join('');
        var badgesHtml = badgeItems ? '<div class="dir-card-badges">' + badgeItems + '</div>' : '';

        var trustHtml = trustTierHtml(m, 'card');

        // Online/offline status
        var statusHtml = '';
        if (m.last_active > 0) {
            var diff = Math.floor(Date.now() / 1000) - m.last_active;
            var statusClass, statusText;
            if (diff < 900) { statusClass = 'online'; statusText = 'online'; }
            else if (diff < 3600) { statusClass = 'recent'; statusText = Math.floor(diff / 60) + 'm ago'; }
            else { statusClass = 'offline'; statusText = _timeAgo(m.last_active); }
            statusHtml = '<div class="dir-card-status ' + statusClass + '">' +
                '<span class="dir-status-dot"></span>' +
                '<span class="dir-status-text">' + statusText + '</span>' +
            '</div>';
        }

        var bioText = escHtml(m.about || '');

        var safeCardUrl = m.card_url && m.card_url.charAt(0) === '/' ? escHtml(m.card_url) : '#';

        // Always render badges placeholder so grid rows stay consistent
        if (!badgesHtml) badgesHtml = '<div class="dir-card-badges"></div>';

        var tierClass = m.trust_tier ? ' tier-' + m.trust_tier : '';
        return '<div class="dir-card' + tierClass + '" data-url="' + safeCardUrl + '" data-pubkey="' + escHtml(m.pubkey) + '">' +
            '<div class="dir-card-banner">' + statusHtml + '</div>' +
            '<div class="dir-card-body">' +
                '<div class="dir-card-avatar-wrap"><div class="dir-avatar-ring">' + makeAvatarHtml(m, 'lg') + '</div></div>' +
                '<a href="' + safeCardUrl + '" class="dir-card-name">' + escHtml(m.name || 'Anonymous') + (m.self_signed ? ' <span class="dir-verified" title="Self-signed listing — this member verified their identity with their own Nostr keys">&#10003;</span>' : '') + '</a>' +
                '<div class="dir-card-nip05">' + escHtml(username) + '</div>' +
                trustHtml +
                '<div class="dir-card-bio">' + bioText + '</div>' +
                badgesHtml +
            '</div>' +
        '</div>';
    }

    // -----------------------------------------------------------------------
    // Render dispatcher
    // -----------------------------------------------------------------------
    function renderMember(m, index) {
        var s = getTermState();
        if (s.view === 'grid') {
            return renderCard(m);
        }
        return renderRow(m, index + 1);
    }

    function renderTableHeader() {
        return '<div class="dir-row dir-row-header">' +
            '<div class="dir-row-rank">#</div>' +
            '<div class="dir-row-avatar"></div>' +
            '<div class="dir-row-identity">Member</div>' +
            '<div class="dir-row-badges">Badges</div>' +
            '<div class="dir-row-trust-header">GrapeRank</div>' +
            '<div class="dir-row-stats">Activity</div>' +
        '</div>';
    }

    function _timeAgo(ts) {
        var diff = Math.floor(Date.now() / 1000) - ts;
        if (diff < 60) return 'just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        if (diff < 604800) return Math.floor(diff / 86400) + 'd ago';
        return Math.floor(diff / 604800) + 'w ago';
    }

    // -----------------------------------------------------------------------
    // Skeleton
    // -----------------------------------------------------------------------
    function showSkeleton() {
        var s = getTermState();
        if (s.view === 'grid') {
            var sk = '<div class="dir-card dir-skeleton"><div class="dir-card-banner"></div><div class="dir-card-body"><div class="dir-skeleton-avatar"></div><div class="dir-skeleton-line" style="width:60%"></div><div class="dir-skeleton-line" style="width:40%"></div><div class="dir-skeleton-line" style="width:80%"></div><div></div></div></div>';
            grid.innerHTML = sk + sk + sk;
        } else {
            var skr = '<div class="dir-row dir-skeleton-row"><div class="dir-skeleton-circle"></div><div class="dir-skeleton-line" style="width:40%"></div><div class="dir-skeleton-line" style="width:20%"></div></div>';
            grid.innerHTML = skr + skr + skr + skr + skr;
        }
        grid.style.display = '';
    }

    // -----------------------------------------------------------------------
    // Fetch directory
    // -----------------------------------------------------------------------
    function fetchDirectory(append) {
        if (isLoading) return;
        if (append && allLoaded) return;
        isLoading = true;
        sentinelEl.classList.add('loading');

        var s = getTermState();
        var pageSize = s.view === 'grid' ? 24 : 50;

        if (!append) {
            currentPage = 1;
            allLoaded = false;
            if (loadedMembers.length === 0) {
                showSkeleton();
            } else {
                grid.style.opacity = '0.5';
            }
            loadedMembers = [];
        }

        var params = new URLSearchParams({
            page: currentPage,
            limit: pageSize,
            sort: s.sort,
        });
        if (s.badge) params.set('badge', s.badge);
        if (s.search) params.set('search', s.search);
        if (s.tags.length > 0) params.set('tag', s.tags.join(','));
        if (activeCluster !== null) params.set('cluster', activeCluster);

        // Pass observer npub + hops for personalized trust view (free for everyone)
        if (observerNpub) {
            params.set('observer', observerNpub);
            params.set('hops', currentHops);
        }

        var usePrefetch = (!observerNpub && !append && currentPage === 1 && s.sort === 'trust' && !s.badge && !s.search && s.tags.length === 0 && window.__dirPrefetch && window.__dirPrefetch.members);
        var dataPromise = usePrefetch
            ? window.__dirPrefetch.members.then(function(d) { window.__dirPrefetch.members = null; return d; })
            : fetch(API_BASE + '/api/directory?' + params).then(function(r) {
                if (!r.ok) throw new Error('Failed');
                return r.json();
            });

        dataPromise.then(function(data) {
            if (!data) throw new Error('Failed');
            totalPages = data.pages || 1;
            totalMembers = data.total || 0;
            countEl.textContent = totalMembers + ' member' + (totalMembers !== 1 ? 's' : '');

            // If we got a personalized response, note it
            if (data.personalized) {
                grid.classList.add('dir-personalized');
            } else {
                grid.classList.remove('dir-personalized');
            }

            applyView();

            if (data.members.length === 0 && totalMembers === 0 && !s.search && !s.badge && s.tags.length === 0) {
                grid.innerHTML = '';
                grid.style.display = 'none';
                emptyEl.style.display = '';
            } else if (!append && data.members.length === 0) {
                grid.innerHTML = '<p style="text-align:center;color:var(--text-muted);padding:2rem;grid-column:1/-1;">No matches found.</p>';
                grid.style.display = '';
                emptyEl.style.display = 'none';
            } else {
                var startIdx = loadedMembers.length;
                if (append) {
                    loadedMembers = loadedMembers.concat(data.members);
                } else {
                    loadedMembers = data.members;
                    startIdx = 0;
                    renderedMemberCount = 0;
                }
                window.__dirLoadedMembers = loadedMembers;
                var html = data.members.map(function(m, i) {
                    return renderMember(m, startIdx + i);
                }).join('');
                if (append) {
                    grid.insertAdjacentHTML('beforeend', html);
                } else {
                    var header = (s.view !== 'grid') ? renderTableHeader() : '';
                    grid.innerHTML = header + html;
                }
                renderedMemberCount += data.members.length;
                if (renderedMemberCount >= MAX_RENDERED_MEMBERS && currentPage < totalPages) {
                    allLoaded = true;
                    sentinelEl.textContent = 'Refine filters to narrow more than ' + MAX_RENDERED_MEMBERS + ' results.';
                } else {
                    sentinelEl.textContent = '';
                }
                grid.style.display = '';
                emptyEl.style.display = 'none';
            }

            if (currentPage >= totalPages) {
                allLoaded = true;
            }
        }).catch(function() {
            grid.style.opacity = '1';
            if (!append) {
                grid.innerHTML = '<p style="text-align:center;color:var(--text-muted);padding:2rem;grid-column:1/-1;">Failed to load directory. Try refreshing.</p>';
            }
        }).finally(function() {
            isLoading = false;
            grid.style.opacity = '1';
            sentinelEl.classList.remove('loading');
            // If observer was set while we were loading, refetch with personalization
            if (pendingObserver) {
                pendingObserver = null;
                fetchDirectory(false);
            }
        });
    }

    // -----------------------------------------------------------------------
    // Infinite scroll
    // -----------------------------------------------------------------------
    var observer = new IntersectionObserver(function(entries) {
        if (entries[0].isIntersecting && !isLoading && !allLoaded) {
            currentPage++;
            fetchDirectory(true);
        }
    }, { rootMargin: '200px' });
    observer.observe(sentinelEl);

    // -----------------------------------------------------------------------
    // Terminal integration
    // -----------------------------------------------------------------------
    function onTerminalChange(newState) {
        syncSortTabs();
        applyView();
        fetchDirectory(false);
    }

    // Hook into terminal
    if (window.__dirTerminal) {
        window.__dirTerminal.onChange = onTerminalChange;
    }

    // -----------------------------------------------------------------------
    // Sort tabs
    // -----------------------------------------------------------------------
    var sortTabs = document.querySelectorAll('.dfilter-sort-btn');
    function syncSortTabs() {
        sortTabs = document.querySelectorAll('.dfilter-sort-btn');
        var s = getTermState();
        for (var i = 0; i < sortTabs.length; i++) {
            sortTabs[i].classList.toggle('active', sortTabs[i].dataset.sort === s.sort);
        }
    }

    // -----------------------------------------------------------------------
    // Bio toggle + card click delegation
    // -----------------------------------------------------------------------
    grid.addEventListener('click', function(e) {
        var card = e.target.closest('.dir-card[data-url]');
        if (card && !e.target.closest('a')) {
            var url = card.getAttribute('data-url');
            if (url && url.charAt(0) === '/') window.location.href = url;
        }
    });

    // -----------------------------------------------------------------------
    // Delegated image error handler (replaces inline onerror)
    // -----------------------------------------------------------------------
    grid.addEventListener('error', function(e) {
        if (e.target.tagName === 'IMG' && e.target.hasAttribute('data-fallback')) {
            e.target.style.display = 'none';
            var fb = e.target.nextElementSibling;
            if (fb) fb.style.display = 'flex';
        }
    }, true);

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------
    // Expose observer setter so other directory controls can update it.
    var pendingObserver = null;
    window.__dirSetObserver = function(npub) {
        if (npub && npub.startsWith('npub1')) {
            observerNpub = npub;
            localStorage.setItem('directory_npub', npub);
            if (isLoading) {
                // Initial fetch still in-flight — queue a refetch after it finishes
                pendingObserver = npub;
            } else {
                fetchDirectory(false);
            }
        }
    };
    window.__dirGetObserver = function() { return observerNpub; };

    setupPaidUI();
    fetchStats();
    fetchClusters();
    fetchDirectory(false);
    if (window.__dirTerminal) {
        window.__dirTerminal.onChange = onTerminalChange;
    }
})();
