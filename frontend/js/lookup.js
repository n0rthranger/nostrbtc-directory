(function() {
    'use strict';

    var input = document.getElementById('lookup-input');
    var btn = document.getElementById('lookup-btn');
    var result = document.getElementById('lookup-result');
    var connectArea = document.getElementById('wot-connect-area');
    if (!input || !btn || !result) return;

    // Delegated image error handler — replaces all inline onerror attributes
    document.addEventListener('error', function(e) {
        var el = e.target;
        if (el.tagName !== 'IMG') return;
        var fb = el.getAttribute('data-fallback');
        if (fb === 'remove') { el.remove(); return; }
        if (fb === 'hide' && el.nextElementSibling) {
            el.style.display = 'none';
            el.nextElementSibling.style.display = 'flex';
        }
        // Mini avatar: replace with gradient fallback
        if (fb === 'gradient') {
            var span = document.createElement('span');
            span.className = 'lu-mini-av-fb';
            span.style.cssText = el.getAttribute('data-fb-style') || '';
            span.textContent = el.getAttribute('data-fb-char') || '?';
            el.replaceWith(span);
        }
    }, true);

    // --- Utilities ---
    function esc(s) { return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
    function fmtK(n) { return n >= 10000 ? (n/1000).toFixed(1) + 'K' : (n || 0).toLocaleString(); }
    function fmtSats(n) {
        if (!n) return '0';
        if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
        if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
        return n.toLocaleString();
    }
    function isNpub(v) { return v.startsWith('npub1') && v.length >= 60; }
    function isHex(v) { return v.length === 64 && /^[0-9a-f]+$/.test(v); }
    function isNip05(v) { return v.indexOf('@') > 0; }
    function truncNpub(n) { return n.slice(0, 12) + '...' + n.slice(-6); }

    function gradientAvatar(key) {
        var colors = ['#f7931a','#8b5cf6','#06b6d4','#10b981','#f43f5e','#eab308','#6366f1','#ec4899'];
        var h = 0;
        for (var i = 0; i < Math.min(key.length, 8); i++) h = ((h << 5) - h + key.charCodeAt(i)) | 0;
        return 'linear-gradient(135deg, ' + colors[Math.abs(h) % colors.length] + ', ' + colors[Math.abs(h >> 4) % colors.length] + ')';
    }

    function miniAvatar(pic, name, pk, size) {
        size = size || 28;
        if (pic) {
            return '<img class="lu-mini-av" style="width:' + size + 'px;height:' + size + 'px" src="' + esc(pic) + '" alt="" title="' + esc(name) + '" data-fallback="gradient" data-fb-style="background:' + gradientAvatar(pk || name).replace(/"/g, '') + ';width:' + size + 'px;height:' + size + 'px" data-fb-char="' + esc((name||'?')[0].toUpperCase()) + '">';
        }
        return '<span class="lu-mini-av-fb" style="background:' + gradientAvatar(pk || name || '') + ';width:' + size + 'px;height:' + size + 'px" title="' + esc(name) + '">' + esc((name || '?')[0].toUpperCase()) + '</span>';
    }

    // --- Bech32 ---
    var CHARSET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
    function bech32Polymod(values) {
        var GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
        var chk = 1;
        for (var i = 0; i < values.length; i++) { var b = chk >> 25; chk = ((chk & 0x1ffffff) << 5) ^ values[i]; for (var j = 0; j < 5; j++) if ((b >> j) & 1) chk ^= GEN[j]; }
        return chk;
    }
    function hexToNpub(hex) {
        var bytes = [];
        for (var i = 0; i < hex.length; i += 2) bytes.push(parseInt(hex.substr(i, 2), 16));
        var data5 = [], acc = 0, bits = 0;
        for (var i = 0; i < bytes.length; i++) { acc = (acc << 8) | bytes[i]; bits += 8; while (bits >= 5) { bits -= 5; data5.push((acc >> bits) & 31); } }
        if (bits > 0) data5.push((acc << (5 - bits)) & 31);
        var hrp = 'npub', expanded = [];
        for (var i = 0; i < hrp.length; i++) expanded.push(hrp.charCodeAt(i) >> 5);
        expanded.push(0);
        for (var i = 0; i < hrp.length; i++) expanded.push(hrp.charCodeAt(i) & 31);
        var combined = data5.slice();
        var mod = bech32Polymod(expanded.concat(combined).concat([0,0,0,0,0,0])) ^ 1;
        for (var i = 0; i < 6; i++) combined.push((mod >> (5 * (5 - i))) & 31);
        return hrp + '1' + combined.map(function(d) { return CHARSET[d]; }).join('');
    }
    function npubToHex(npub) {
        var hrp = 'npub';
        var data5 = [];
        for (var i = 5; i < npub.length - 6; i++) {
            var idx = CHARSET.indexOf(npub[i]);
            if (idx < 0) return '';
            data5.push(idx);
        }
        var checksum = [];
        for (var c = npub.length - 6; c < npub.length; c++) {
            var chk = CHARSET.indexOf(npub[c]);
            if (chk < 0) return '';
            checksum.push(chk);
        }
        var expanded = [];
        for (var h = 0; h < hrp.length; h++) expanded.push(hrp.charCodeAt(h) >> 5);
        expanded.push(0);
        for (var hh = 0; hh < hrp.length; hh++) expanded.push(hrp.charCodeAt(hh) & 31);
        if (bech32Polymod(expanded.concat(data5).concat(checksum)) !== 1) return '';
        var acc = 0, bits = 0, bytes = [];
        for (var j = 0; j < data5.length; j++) {
            acc = (acc << 5) | data5[j]; bits += 5;
            while (bits >= 8) { bits -= 8; bytes.push((acc >> bits) & 255); }
        }
        return bytes.map(function(b) { return b.toString(16).padStart(2, '0'); }).join('');
    }

    // --- Connection state ---
    var connected = {
        npub: localStorage.getItem('directory_npub') || '',
        hex: localStorage.getItem('directory_hex_pubkey') || '',
        via: localStorage.getItem('directory_connected_via') || '',
        isMember: false,
        selfSigned: false,
        profile: null
    };

    var _lastLookupTarget = '';
    var _trustPollActive = false;
    var _trustPollRetries = 0;
    var _trustPollTimer = null;

    function hasNip07() { return !!(window.nostr && window.nostr.signEvent && window.nostr.getPublicKey); }
    function isConnected() { return !!(connected.hex && connected.npub); }

    function refreshLookup() {
        if (!_lastLookupTarget || !isConnected()) return;
        var observerParam = '&observer=' + encodeURIComponent(connected.npub);
        fetch('/api/directory/trust-lookup?target=' + encodeURIComponent(_lastLookupTarget) + observerParam)
            .then(function(r) { return r.ok ? r.json() : null; })
            .then(function(d) { if (d) renderResult(d); })
            .catch(function(err) { console.warn('Trust refresh failed:', err && err.message ? err.message : err); });
    }

    function saveConnection(npub, hex, via) {
        connected.npub = npub;
        connected.hex = hex;
        connected.via = via;
        localStorage.setItem('directory_npub', npub);
        localStorage.setItem('directory_hex_pubkey', hex);
        localStorage.setItem('directory_connected_via', via);
    }

    function clearConnection() {
        connected.npub = '';
        connected.hex = '';
        connected.via = '';
        connected.isMember = false;
        connected.selfSigned = false;
        connected.profile = null;
        _trustComputedOnce = false;
        _computingTrust = false;
        _trustPollActive = false;
        if (_trustPollTimer) { clearTimeout(_trustPollTimer); _trustPollTimer = null; }
        localStorage.removeItem('directory_npub');
        localStorage.removeItem('directory_hex_pubkey');
        localStorage.removeItem('directory_connected_via');
    }

    function updateDirCta() {
        var cta = document.querySelector('.dir-cta');
        if (cta) cta.style.display = connected.isMember ? 'none' : '';
    }

    // --- Trust tier config ---
    var TIERS = {
        highly_trusted: { label: 'Highly Trusted', color: '#F7931A', glow: 'rgba(247,147,26,0.25)', desc: 'Strongly trusted by your close network. Multiple independent trust paths.' },
        trusted:        { label: 'Trusted',        color: '#c084fc', glow: 'rgba(192,132,252,0.2)',  desc: 'Followed by people in your network. Solid trust signal.' },
        neutral:        { label: 'Neutral',        color: '#a78bfa', glow: 'rgba(167,139,250,0.15)', desc: 'Known in your extended network but limited direct connection.' },
        low_trust:      { label: 'Low Trust',      color: '#7c3aed', glow: 'rgba(124,58,237,0.15)',  desc: 'At the edges of your network. Minimal trust signal.' },
        unverified:     { label: 'Unverified',     color: '#4c1d95', glow: 'rgba(76,29,149,0.1)',    desc: 'No connection found in your network.' },
        unknown:        { label: 'Unknown',        color: '#4c1d95', glow: 'rgba(76,29,149,0.1)',    desc: 'No trust data computed yet.' }
    };

    function tierInfo(tier) { return TIERS[tier] || TIERS.unknown; }

    // --- Sparkline SVG (trust score trend) ---
    function sparklineSvg(history) {
        if (!history || history.length < 2) return '';
        var vals = history.map(function(h) { return h.trust_score || 0; });
        var mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals);
        if (mx === mn) mx = mn + 1;
        var w = 120, h = 28, pad = 2;
        var pts = [];
        for (var i = 0; i < vals.length; i++) {
            var x = pad + (i / (vals.length - 1)) * (w - pad * 2);
            var y = h - pad - ((vals[i] - mn) / (mx - mn)) * (h - pad * 2);
            pts.push(x.toFixed(1) + ',' + y.toFixed(1));
        }
        var last = vals[vals.length - 1];
        var color = last >= 0.7 ? '#F7931A' : last >= 0.4 ? '#2DD4BF' : '#6B7280';
        var areaPath = 'M' + pts[0] + ' L' + pts.join(' L') + ' L' + (w - pad) + ',' + (h - pad) + ' L' + pad + ',' + (h - pad) + ' Z';
        return '<svg class="lu-sparkline" width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + ' ' + h + '">' +
            '<defs><linearGradient id="spkg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="' + color + '" stop-opacity="0.2"/><stop offset="100%" stop-color="' + color + '" stop-opacity="0"/></linearGradient></defs>' +
            '<path d="' + areaPath + '" fill="url(#spkg)"/>' +
            '<polyline points="' + pts.join(' ') + '" fill="none" stroke="' + color + '" stroke-width="1.5" stroke-linejoin="round"/>' +
            '<circle cx="' + pts[pts.length - 1].split(',')[0] + '" cy="' + pts[pts.length - 1].split(',')[1] + '" r="2.5" fill="' + color + '"/>' +
        '</svg>';
    }

    // --- Trust depth ---
    function getTrustDepth() {
        return parseInt(localStorage.getItem('directory_trust_depth') || '3', 10);
    }
    function setTrustDepth(v) {
        localStorage.setItem('directory_trust_depth', v);
    }

    // --- Connect UI ---
    function renderConnectArea() {
        if (!connectArea) return;

        if (isConnected()) {
            renderConnectedState();
        } else {
            renderSignInForm();
        }
    }

    function renderSignInForm() {
        var nip07 = hasNip07();
        var html =
            '<div class="wot-connect-methods">';

        if (nip07) {
            html +=
                '<button type="button" id="wot-ext-btn" class="wot-ext-btn">' +
                    'Connect Extension' +
                '</button>' +
                '<span class="wot-ext-hint">NIP-07 signer detected</span>';
        } else {
            html +=
                '<div class="wot-connect-field">' +
                    '<input type="text" id="wot-npub-input" class="wot-input" placeholder="npub1..." autocomplete="off" spellcheck="false">' +
                    '<button type="button" id="wot-npub-go" class="wot-connect-btn">Connect</button>' +
                '</div>' +
                '<span class="wot-ext-hint">Paste your npub to connect. Install a <a href="https://github.com/nicehash/nos2x" target="_blank" rel="noopener" style="color:var(--neon-purple)">NIP-07 extension</a> to sign verified listings.</span>';
        }

        html += '</div>';
        connectArea.innerHTML = html;

        if (nip07) {
            document.getElementById('wot-ext-btn').onclick = doExtensionConnect;
        } else {
            var npubInput = document.getElementById('wot-npub-input');
            var npubGo = document.getElementById('wot-npub-go');
            npubGo.onclick = function() { doManualConnect(npubInput.value.trim()); };
            npubInput.addEventListener('keydown', function(e) {
                if (e.key === 'Enter') { e.preventDefault(); doManualConnect(npubInput.value.trim()); }
            });
            npubInput.addEventListener('paste', function() {
                setTimeout(function() { doManualConnect(npubInput.value.trim()); }, 80);
            });
        }
    }

    function renderConnectedState() {
        var p = connected.profile || {};
        var name = p.name || 'Anonymous';
        var picture = p.picture || '';
        var nip05 = p.nip05_display || '';
        var grad = gradientAvatar(connected.hex);

        // Avatar
        var avHtml;
        if (picture && picture.startsWith('http')) {
            avHtml = '<img class="mp-pic" src="' + esc(picture) + '" alt="" data-fallback="hide">' +
                '<span class="mp-pic mp-pic-fb" style="display:none;background:' + grad + '">' + esc(name[0].toUpperCase()) + '</span>';
        } else {
            avHtml = '<span class="mp-pic mp-pic-fb" style="background:' + grad + '">' + esc(name[0].toUpperCase()) + '</span>';
        }

        // Badges
        var badgeHtml = '';
        var badges = p.badges || [];
        var badgeMap = {
            'relay-subscriber': { label: 'Relay Subscriber', cls: 'mp-badge-relay' },
            'nip05-live': { label: 'Verified', cls: 'mp-badge-verified' },
            'lightning-reachable': { label: 'Lightning', cls: 'mp-badge-lightning' }
        };
        if (badges.length > 0) {
            badgeHtml = '<div class="mp-badges">';
            for (var i = 0; i < badges.length; i++) {
                var bDef = badgeMap[badges[i]] || { label: badges[i].replace(/-/g, ' '), cls: '' };
                badgeHtml += '<span class="mp-badge ' + bDef.cls + '">' + esc(bDef.label) + '</span>';
            }
            badgeHtml += '</div>';
        }

        // Self-sign badge (compact — only shows verified status or a small action link)
        var selfSignHtml = '';
        if (connected.isMember && connected.selfSigned) {
            selfSignHtml = '<span class="mp-selfsign-badge mp-selfsign-done" title="Your listing is self-signed and cryptographically verifiable">\u2713 Verified</span>';
        } else if (connected.isMember && !connected.selfSigned && connected.via === 'extension') {
            selfSignHtml = '<button type="button" id="wot-claim-btn" class="mp-selfsign-badge mp-selfsign-btn" title="Sign your directory listing with your own keys">\u270D Verify listing</button>';
        }

        var html =
            '<div class="wot-my-profile">' +
                '<div class="mp">' +
                    '<div class="mp-top">' +
                        '<div class="mp-avatar" style="--ring-color:var(--neon-purple)">' + avHtml + '</div>' +
                        '<div class="mp-info">' +
                            '<div class="mp-name">' + esc(name) + '</div>' +
                            (nip05 ? '<div class="mp-nip05">' + esc(nip05) + '</div>' : '') +
                            '<div class="mp-npub">' + esc(truncNpub(connected.npub)) + '</div>' +
                            badgeHtml +
                            selfSignHtml +
                        '</div>' +
                    '</div>' +
                    '<div class="mp-stats">' +
                        '<span class="mp-stat"><strong>' + fmtK(connected._followers || 0) + '</strong> followers</span>' +
                        '<span class="mp-stat"><strong>' + fmtK(connected._following || 0) + '</strong> following</span>' +
                        (connected._muting > 0 ? '<span class="mp-stat"><strong>' + connected._muting + '</strong> muting</span>' : '') +
                    '</div>' +
                '</div>' +
                '<div style="display:flex;align-items:center;gap:0.5rem;margin-top:0.6rem;flex-wrap:wrap">' +
                    '<button type="button" id="wot-disconnect" class="wot-disconnect">Disconnect</button>' +
                    '<button type="button" id="wot-discover" class="wot-discover-btn" style="margin:0">Discover people \u2192</button>' +
                '</div>' +
            '</div>';

        connectArea.innerHTML = html;

        document.getElementById('wot-disconnect').onclick = function() {
            clearConnection();
            updateDirCta();
            renderConnectArea();
            result.style.display = 'none';
        };
        document.getElementById('wot-discover').onclick = loadDiscover;

        var claim = document.getElementById('wot-claim-btn');
        if (claim) claim.onclick = doClaimListing;
    }

    // --- Connect flows ---
    function doExtensionConnect() {
        if (!hasNip07()) return;
        window.nostr.getPublicKey().then(function(hex) {
            if (!hex || hex.length !== 64 || !/^[0-9a-f]+$/.test(hex)) return;
            var npub = hexToNpub(hex);
            saveConnection(npub, hex, 'extension');
            fetchMyProfile();
            refreshLookup();
        }).catch(function(err) {
            console.warn('NIP-07 connection failed:', err && err.message ? err.message : err);
        });
    }

    function doManualConnect(val) {
        if (!val) return;
        var hex = '';
        if (isNpub(val)) {
            hex = npubToHex(val);
        } else if (isHex(val)) {
            hex = val;
            val = hexToNpub(hex);
        } else {
            return; // not valid
        }
        if (!hex) return;
        saveConnection(val, hex, 'manual');
        fetchMyProfile();
        refreshLookup();
    }

    function fetchMyProfile() {
        connectArea.innerHTML = '<div class="mp-loading"><div class="sc-loading-sweep"></div></div>';

        fetch('/api/directory/trust-lookup?target=' + encodeURIComponent(connected.npub) + '&observer=' + encodeURIComponent(connected.npub))
            .then(function(r) { return r.ok ? r.json() : null; })
            .then(function(d) {
                if (d) {
                    connected.isMember = d.is_member || false;
                    connected.selfSigned = (d.profile && d.profile.self_signed) || false;
                    connected.profile = d.profile || null;
                    connected._trustScore = d.trust_score;
                    connected._trustTier = d.trust_tier || 'unknown';
                    connected._followers = d.followers_count || 0;
                    connected._following = d.following_count || 0;
                    connected._muting = d.muting_count || 0;

                    // If trust not computed, trigger on-demand computation
                    if (d.trust_tier === 'unknown' || (d.trust_score === 0 && d.trust_tier === 'unknown')) {
                        triggerTrustComputation();
                    }
                }
                updateDirCta();
                renderConnectArea();
            })
            .catch(function() { renderConnectArea(); });
    }

    var _computingTrust = false;
    var _trustComputedOnce = false;
    function triggerTrustComputation() {
        if (_computingTrust || _trustComputedOnce) return;
        _computingTrust = true;
        fetch('/api/directory/compute-trust', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ npub: connected.npub })
        })
        .then(function(r) { return r.json(); })
        .then(function(d) {
            _computingTrust = false;
            if (d && d.status === 'ready') {
                _trustComputedOnce = true;
                // Cancel any active poll — computation is done, fresh data incoming
                _trustPollActive = false;
                _trustPollRetries = 0;
                if (_trustPollTimer) { clearTimeout(_trustPollTimer); _trustPollTimer = null; }
                refreshLookup();
            }
        })
        .catch(function() { _computingTrust = false; });
    }

    // --- Claim listing (kind 9999 self-sign) ---
    function doClaimListing() {
        var claim = document.getElementById('wot-claim-btn');
        if (!claim || !hasNip07()) return;
        claim.disabled = true;
        claim.textContent = 'Signing...';

        fetch('/api/directory/list-header')
            .then(function(r) { if (!r.ok) throw new Error(); return r.json(); })
            .then(function(hdr) {
                return window.nostr.signEvent({
                    kind: 9999, created_at: Math.floor(Date.now() / 1000),
                    tags: [['z', hdr.list_header_event_id], ['p', connected.hex]], content: ''
                });
            })
            .then(function(ev) {
                if (!ev || !ev.id || !ev.sig) throw new Error();
                return publishToRelay(ev);
            })
            .then(function() {
                connected.selfSigned = true;
                renderConnectArea();
            })
            .catch(function() { claim.textContent = 'Failed \u2014 retry'; claim.disabled = false; });
    }

    var _PUBLIC_RELAYS = [
        'wss://relay.damus.io', 'wss://relay.primal.net', 'wss://nos.lol',
        'wss://relay.nostr.net', 'wss://offchain.pub'
    ];

    function _publishToSingleRelay(url, event) {
        return new Promise(function(resolve, reject) {
            var ws;
            try { ws = new WebSocket(url); } catch(e) { reject(e); return; }
            var t = setTimeout(function() { ws.close(); reject(new Error('Timeout')); }, 10000);
            ws.onopen = function() { ws.send(JSON.stringify(['EVENT', event])); };
            ws.onmessage = function(m) {
                try { var d = JSON.parse(m.data); if (d[0] === 'OK' && d[1] === event.id) { clearTimeout(t); ws.close(); d[2] ? resolve() : reject(new Error(d[3] || 'Rejected')); } } catch(e) { reject(e); }
            };
            ws.onerror = function() { clearTimeout(t); reject(new Error('WS error')); };
        });
    }

    function publishToRelay(event) {
        // Must succeed on our relay; broadcast to public relays in parallel (best-effort)
        var ownRelay = (location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host;
        _PUBLIC_RELAYS.forEach(function(url) { _publishToSingleRelay(url, event).catch(function(err) { console.warn('Public relay publish failed:', url, err && err.message ? err.message : err); }); });
        return _publishToSingleRelay(ownRelay, event);
    }

    // --- Search ---
    function doSearch() {
        var v = input.value.trim();
        if (!v) return;
        if (!isNpub(v) && !isHex(v) && !isNip05(v)) {
            result.style.display = '';
            result.innerHTML = '<div class="lu-error">Enter an npub, NIP-05 (user@domain), or hex pubkey.</div>';
            return;
        }
        result.style.display = '';
        result.innerHTML =
            '<div class="lu-scanning">' +
                '<div class="lu-radar">' +
                    '<div class="lu-radar-circle r1"></div><div class="lu-radar-circle r2"></div><div class="lu-radar-circle r3"></div>' +
                    '<div class="lu-radar-sweep"></div>' +
                    '<div class="lu-radar-dot"></div>' +
                    '<div class="lu-radar-blip"></div><div class="lu-radar-blip"></div><div class="lu-radar-blip"></div>' +
                '</div>' +
                '<div class="lu-scan-text">Scanning relays\u2026</div>' +
            '</div>';

        _lastLookupTarget = v;
        var observerParam = isConnected() ? ('&observer=' + encodeURIComponent(connected.npub)) : '';
        fetch('/api/directory/trust-lookup?target=' + encodeURIComponent(v) + observerParam)
            .then(function(r) {
                if (!r.ok) return r.json().then(function(d) { throw new Error(d.error || 'Not found'); });
                return r.json();
            })
            .then(renderResult)
            .catch(function(err) {
                result.innerHTML =
                    '<div class="lu-not-found">' +
                        '<div class="lu-nf-title">Identity not found</div>' +
                        '<p class="lu-nf-desc">Could not resolve this identity. Check the npub or NIP-05 and try again.</p>' +
                        '<button type="button" class="lu-btn" id="lu-retry">Try Again</button>' +
                    '</div>';
                document.getElementById('lu-retry').onclick = function() { input.focus(); result.style.display = 'none'; };
            });
    }

    // --- Trust ring SVG ---
    function trustRingSvg(pct, color, size) {
        size = size || 90;
        var r = (size - 8) / 2;
        var circ = 2 * Math.PI * r;
        var dash = circ * (pct / 100);
        var gap = circ - dash;
        var cx = size / 2, cy = size / 2;
        return '<svg class="lu-ring-svg" width="' + size + '" height="' + size + '" viewBox="0 0 ' + size + ' ' + size + '">' +
            '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="rgba(255,255,255,0.04)" stroke-width="5"/>' +
            '<circle class="lu-ring-arc" cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + color + '" stroke-width="5" ' +
                'stroke-dasharray="0 ' + circ.toFixed(1) + '" data-target-dash="' + dash.toFixed(1) + ' ' + gap.toFixed(1) + '" ' +
                'stroke-linecap="round" transform="rotate(-90 ' + cx + ' ' + cy + ')" style="filter:drop-shadow(0 0 6px ' + color + ');transition:stroke-dasharray 1s cubic-bezier(0.16,1,0.3,1)"/>' +
        '</svg>';
    }

    // --- Render result card ---
    /**
     * Render the trust lookup dossier returned by /api/directory/trust-lookup.
     */
    function renderResult(data) {
        var p = data.profile || {};
        var name = p.name || 'Unknown';
        var nip05 = p.nip05_display || '';
        var picture = p.picture || '';
        var about = p.about || '';
        var npub = p.npub || (data.target ? hexToNpub(data.target) : '');
        var hexPk = data.target || '';
        var isMember = data.is_member;
        var selfSigned = p.self_signed || false;
        var cardUrl = p.card_url || '';
        var grad = gradientAvatar(hexPk || name);

        var isSelf = isConnected() && hexPk === connected.hex;
        var hasTrust = data.trust_score !== null && data.trust_tier !== null && data.trust_tier !== 'unknown';
        var tier = hasTrust ? tierInfo(data.trust_tier) : null;
        var glowColor = (hasTrust && tier) ? tier.color : '#222';

        // Avatar
        var avatarHtml;
        if (picture && picture.startsWith('http')) {
            avatarHtml = '<img class="lu-pic" src="' + esc(picture) + '" alt="" data-fallback="hide">' +
                '<span class="lu-pic lu-pic-fb" style="display:none;background:' + grad + '">' + esc(name[0].toUpperCase()) + '</span>';
        } else {
            avatarHtml = '<span class="lu-pic lu-pic-fb" style="background:' + grad + '">' + esc(name[0].toUpperCase()) + '</span>';
        }

        // Name
        var nameHtml;
        if (isMember && cardUrl) {
            nameHtml = '<a href="' + esc(cardUrl) + '" class="lu-name lu-name-link">' + esc(name) + '</a>';
        } else {
            nameHtml = '<div class="lu-name">' + esc(name) + '</div>';
        }

        // Trust ring (right side of zone 1)
        // Trust ring — cyberpunk style for all states
        var trustRingHtml = '';
        if (hasTrust) {
            var pct = Math.round((data.trust_score || 0) * 100);
            var hopsLabel = data.trust_hops != null ? data.trust_hops + ' hop' + (data.trust_hops !== 1 ? 's' : '') : '';
            var confidenceLabel = pct >= 70 ? 'High' : pct >= 40 ? 'Moderate' : pct >= 15 ? 'Low' : 'Minimal';
            var displayName = esc(name !== 'Unknown' ? name : truncNpub(npub));
            trustRingHtml =
                '<div class="lu-trust-locked">' +
                    '<div class="lu-locked-ring" style="--tier-color:' + tier.color + ';border-color:' + tier.color + '30;box-shadow:0 0 15px ' + tier.color + '25,0 0 30px ' + tier.color + '10,inset 0 0 20px rgba(0,0,0,0.5)">' +
                        '<div class="lu-locked-glitch" data-text="' + pct + '" style="color:' + tier.color + ';text-shadow:0 0 10px ' + tier.color + ',2px 0 rgba(247,147,26,0.3),-2px 0 rgba(6,182,212,0.3)">' + pct + '</div>' +
                        '<div class="lu-locked-scanline"></div>' +
                    '</div>' +
                    '<div class="lu-locked-label" style="color:' + tier.color + ';text-shadow:0 0 8px ' + tier.color + '66">TRUST SCORE</div>' +
                    '<div class="lu-confidence">' +
                        '<span class="lu-confidence-level" style="color:' + tier.color + '">' + confidenceLabel + ' confidence</span>' +
                        '<span class="lu-confidence-desc">' + pct + '% confidence that <strong>' + displayName + '</strong> is a genuine participant, based on your trusted community\u2019s follows, mutes, and reports.</span>' +
                    '</div>' +
                    '<div class="lu-ring-meta">' +
                        (hopsLabel ? '<span class="lu-ring-hops">' + hopsLabel + '</span>' : '') +
                        '<a href="https://github.com/NosFabrica/brainstorm_graperank_algorithm" target="_blank" rel="noopener" class="lu-ring-source">GrapeRank</a>' +
                    '</div>' +
                '</div>';
        } else {
            if (isSelf) {
                // Self-lookup — GrapeRank doesn't produce self-scores
                trustRingHtml =
                    '<div class="lu-trust-locked">' +
                        '<div class="lu-locked-ring" style="border-color:rgba(139,92,246,0.2);box-shadow:0 0 15px rgba(139,92,246,0.15)">' +
                            '<div class="lu-locked-glitch" data-text="\u2014" style="color:#8b5cf6">\u2014</div>' +
                            '<div class="lu-locked-scanline"></div>' +
                        '</div>' +
                        '<div class="lu-locked-label" style="color:#8b5cf6">YOUR IDENTITY</div>' +
                        '<div class="lu-locked-sub">Trust scores are computed by others looking you up</div>' +
                    '</div>';
            } else if (isConnected()) {
                // Check if score was computed but is genuinely zero (no trust path)
                var wasComputed = data.computed_at || (data.shared_follows_count > 0) || (data.mutual_followers_count > 0);
                if (!wasComputed && _trustComputedOnce) {
                    // No trust path exists — show "not yet rated"
                    trustRingHtml =
                        '<div class="lu-trust-locked">' +
                            '<div class="lu-locked-ring" style="border-color:rgba(255,255,255,0.1);box-shadow:0 0 15px rgba(255,255,255,0.05)">' +
                                '<div class="lu-locked-glitch" data-text="--" style="color:rgba(255,255,255,0.3)">--</div>' +
                                '<div class="lu-locked-scanline"></div>' +
                            '</div>' +
                            '<div class="lu-locked-label" style="color:rgba(255,255,255,0.4)">NO TRUST DATA</div>' +
                            '<div class="lu-locked-sub">No connections found in your trust graph</div>' +
                        '</div>';
                } else {
                    trustRingHtml =
                        '<div class="lu-trust-locked">' +
                            '<div class="lu-locked-ring lu-locked-ring-computing">' +
                                '<div class="lu-locked-glitch lu-locked-glitch-computing" data-text="--">--</div>' +
                                '<div class="lu-locked-scanline"></div>' +
                            '</div>' +
                            '<div class="lu-locked-label">TRUST SCORE</div>' +
                            '<div class="lu-locked-sub">analyzing trust graph</div>' +
                        '</div>';
                }
            } else {
                trustRingHtml =
                    '<div class="lu-trust-locked">' +
                        '<button type="button" class="lu-locked-btn" id="lu-locked-connect" style="padding:0.5rem 1rem;font-size:0.75rem;border-radius:8px">' +
                            '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/></svg> ' +
                            'Connect to unlock trust' +
                        '</button>' +
                    '</div>';
            }
        }

        // Trust path (inline under identity)
        var pathHtml = '';
        var trustPath = data.trust_path || [];
        if (trustPath.length >= 2 && isConnected()) {
            var chain = '';
            for (var i = 0; i < trustPath.length; i++) {
                var tp = trustPath[i];
                var isFirst = (i === 0);
                var isLast = (i === trustPath.length - 1);
                var label = isFirst ? 'You' : (tp.name || truncNpub(hexToNpub(tp.hex_pubkey || tp.pubkey || '')));
                chain += miniAvatar(tp.picture, label, tp.hex_pubkey || tp.pubkey, 20);
                chain += '<span class="lu-ipath-name">' + esc(label) + '</span>';
                if (!isLast) chain += '<span class="lu-ipath-arrow">\u2192</span>';
            }
            pathHtml = '<div class="lu-ipath">' + chain + '</div>';
        }

        // Bio
        var bioHtml = about ? '<p class="lu-bio">' + esc(about.length > 180 ? about.slice(0, 180) + '\u2026' : about) + '</p>' : '';

        // --- Zone 2: Shared connections ---
        var zone2Html = '';
        var sf = data.shared_follows || [];
        var sfCount = data.shared_follows_count || 0;
        var mf = data.mutual_followers || [];
        var mfCount = data.mutual_followers_count || 0;
        var totalShared = sfCount + mfCount;

        function personRow(p, tag) {
            return '<div class="lu-person-row" data-name="' + esc((p.name || '').toLowerCase()) + '">' +
                miniAvatar(p.picture, p.name, p.pubkey, 28) +
                '<div class="lu-person-info">' +
                    '<div class="lu-person-name">' + esc(p.name || (p.pubkey || '').slice(0, 12) + '...') + '</div>' +
                    (p.nip05 ? '<div class="lu-person-nip05">' + esc(p.nip05) + '</div>' : '') +
                '</div>' +
                '<span class="lu-person-tag">' + esc(tag) + '</span>' +
            '</div>';
        }

        function buildPeopleList(people, tag, listId) {
            if (people.length === 0) return '';
            var rows = '';
            for (var i = 0; i < people.length; i++) rows += personRow(people[i], tag);
            return '<div class="lu-people-list" id="' + listId + '">' +
                '<div class="lu-people-search"><input type="text" class="lu-people-filter" placeholder="Filter by name..." autocomplete="off" spellcheck="false"></div>' +
                '<div class="lu-people-rows">' + rows + '</div>' +
            '</div>';
        }

        if (isConnected()) {
            if (totalShared > 0) {
                var avatars = '';
                var allPeople = [], seen = {};
                var combine = mf.concat(sf);
                for (var i = 0; i < combine.length; i++) {
                    if (!seen[combine[i].pubkey]) { seen[combine[i].pubkey] = true; allPeople.push(combine[i]); }
                }
                for (var i = 0; i < Math.min(allPeople.length, 6); i++) {
                    avatars += miniAvatar(allPeople[i].picture, allPeople[i].name, allPeople[i].pubkey, 26);
                }
                var more = totalShared > 6 ? '<span class="lu-shared-more">+' + (totalShared - 6) + '</span>' : '';

                var mfListHtml = mf.length > 0
                    ? '<div class="lu-conn-group"><div class="lu-conn-header" data-toggle="lu-mf-list">Mutual Followers (' + mfCount + ') <span class="lu-sig-arrow">\u25B8</span></div>' + buildPeopleList(mf, 'Follower', 'lu-mf-list') + '</div>'
                    : '';
                var sfListHtml = sf.length > 0
                    ? '<div class="lu-conn-group"><div class="lu-conn-header" data-toggle="lu-sf-list">Mutual Following (' + sfCount + ') <span class="lu-sig-arrow">\u25B8</span></div>' + buildPeopleList(sf, 'Following', 'lu-sf-list') + '</div>'
                    : '';

                zone2Html =
                    '<div class="lu-zone lu-zone-conn">' +
                        '<div class="lu-conn-summary">' +
                            '<div class="lu-conn-avatars">' + avatars + more + '</div>' +
                            '<div class="lu-conn-text">' +
                                '<span class="lu-conn-count">' + totalShared + ' shared connection' + (totalShared !== 1 ? 's' : '') + '</span>' +
                                '<span class="lu-conn-detail">' +
                                    (mfCount > 0 ? mfCount + ' follower' + (mfCount !== 1 ? 's' : '') : '') +
                                    (mfCount > 0 && sfCount > 0 ? ' \u00B7 ' : '') +
                                    (sfCount > 0 ? sfCount + ' following' : '') +
                                '</span>' +
                            '</div>' +
                        '</div>' +
                        mfListHtml + sfListHtml +
                    '</div>';
            } else {
                zone2Html =
                    '<div class="lu-zone lu-zone-conn lu-zone-conn-empty">' +
                        '<span class="lu-conn-empty-text">No shared connections</span>' +
                    '</div>';
            }
        }

        // --- Activity & Account Age zone ---
        var notes = data.notes_count || 0;
        var zapsReceived = data.zaps_received_count || 0;
        var zapsReceivedSats = data.zaps_received_sats || 0;
        var zapsSentSats = data.zaps_sent_sats || 0;
        var firstSeen = data.first_seen || 0;

        var firstSeenHtml = '';
        if (firstSeen > 0) {
            var d = new Date(firstSeen * 1000);
            var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
            var dateStr = months[d.getUTCMonth()] + ' ' + d.getUTCFullYear();
            var ageDays = Math.floor((Date.now() / 1000 - firstSeen) / 86400);
            var ageStr = ageDays >= 365 ? Math.floor(ageDays / 365) + 'y' : ageDays + 'd';
            firstSeenHtml =
                '<div class="lu-act-item">' +
                    '<span class="lu-act-num" style="color:#94a3b8">' + dateStr + '</span>' +
                    '<span class="lu-act-label">on nostr (' + ageStr + ')</span>' +
                '</div>';
        }

        var influenceHtml = '';
        if (hasTrust && data.trust_score != null) {
            var rawScore = data.trust_score.toFixed(2);
            var infColor = data.trust_score >= 0.7 ? '#F7931A' : data.trust_score >= 0.4 ? '#c084fc' : data.trust_score >= 0.15 ? '#a78bfa' : '#7c3aed';
            influenceHtml =
                '<div class="lu-act-item">' +
                    '<span class="lu-act-num" style="color:' + infColor + '">' + rawScore + '</span>' +
                    '<span class="lu-act-label">influence (0\u20131)</span>' +
                '</div>';
        }

        var activityZoneHtml =
            '<div class="lu-zone lu-zone-activity">' +
                '<div class="lu-activity-grid">' +
                    influenceHtml +
                    '<div class="lu-act-item">' +
                        '<span class="lu-act-num lu-act-cyan">' + fmtK(notes) + '</span>' +
                        '<span class="lu-act-label">notes</span>' +
                    '</div>' +
                    '<div class="lu-act-item">' +
                        '<span class="lu-act-num lu-act-sats">' + fmtSats(zapsReceivedSats || zapsReceived) + '</span>' +
                        '<span class="lu-act-label">' + (zapsReceivedSats ? 'sats in' : 'zaps in') + '</span>' +
                    '</div>' +
                (zapsSentSats > 0
                    ? '<div class="lu-act-item">' +
                        '<span class="lu-act-num lu-act-sats">' + fmtSats(zapsSentSats) + '</span>' +
                        '<span class="lu-act-label">sats out</span>' +
                      '</div>'
                    : '') +
                firstSeenHtml +
                '</div>' +
            '</div>';

        // --- Mutual follow indicator ---
        var mutualHtml = '';
        if (isConnected() && data.mutual_follow !== null) {
            if (data.mutual_follow) {
                mutualHtml = '<div class="lu-mutual lu-mutual-yes">You follow each other</div>';
            } else if (data.observer_follows_target) {
                mutualHtml = '<div class="lu-mutual lu-mutual-one">You follow them</div>';
            } else if (data.target_follows_observer) {
                mutualHtml = '<div class="lu-mutual lu-mutual-one">Follows you</div>';
            }
        }

        // --- Zone 3: Signal dashboard (two columns) ---
        var followers = data.followers_count || 0;
        var following = data.following_count || 0;
        var mutedBy = data.muted_by_count || 0;
        var reportedBy = data.reported_by_count || 0;
        var muting = data.muting_count || 0;
        var reporting = data.reporting_count || 0;

        // Left col: reach + audience quality
        var tierDefs = [
            { key: 'highly_trusted', label: 'Highly Trusted', color: '#F7931A' },
            { key: 'trusted',        label: 'Trusted',        color: '#c084fc' },
            { key: 'neutral',        label: 'Neutral',        color: '#a78bfa' },
            { key: 'low_trust',      label: 'Low Trust',      color: '#7c3aed' },
            { key: 'unverified',     label: 'Unverified',     color: '#4c1d95' }
        ];

        var followerTiersHtml = '';
        var ft = data.follower_tiers;
        if (ft && isConnected()) {
            var scored = (ft.highly_trusted || 0) + (ft.trusted || 0) + (ft.neutral || 0) + (ft.low_trust || 0);
            var tierRows = '';
            for (var ti = 0; ti < tierDefs.length; ti++) {
                var td = tierDefs[ti];
                var cnt = ft[td.key] || 0;
                if (cnt === 0) continue;
                var barW = followers > 0 ? Math.max(4, Math.round(cnt / followers * 100)) : 0;
                tierRows +=
                    '<div class="lu-aq-row">' +
                        '<span class="lu-aq-dot" style="background:' + td.color + '"></span>' +
                        '<span class="lu-aq-label">' + td.label + '</span>' +
                        '<span class="lu-aq-bar"><span class="lu-aq-bar-fill" style="width:' + barW + '%;background:' + td.color + '"></span></span>' +
                        '<span class="lu-aq-count">' + cnt + '</span>' +
                    '</div>';
            }
            if (tierRows) {
                followerTiersHtml =
                    '<div class="lu-audience-quality">' +
                        '<div class="lu-aq-header" data-aq-toggle="lu-aq-followers">' +
                            '<div class="lu-aq-title">Audience Quality <span class="lu-sig-arrow">\u25B8</span></div>' +
                            '<div class="lu-aq-sub">' + fmtK(followers) + ' followers</div>' +
                        '</div>' +
                        '<div class="lu-aq-body" id="lu-aq-followers" style="display:none">' + tierRows + '</div>' +
                    '</div>';
            }
        }

        var followingTiersHtml = '';
        var fwt = data.following_tiers;
        if (fwt && isConnected()) {
            var fwtRows = '';
            for (var ti = 0; ti < tierDefs.length; ti++) {
                var td = tierDefs[ti];
                var cnt = fwt[td.key] || 0;
                if (cnt === 0) continue;
                var barW = following > 0 ? Math.max(4, Math.round(cnt / following * 100)) : 0;
                fwtRows +=
                    '<div class="lu-aq-row">' +
                        '<span class="lu-aq-dot" style="background:' + td.color + '"></span>' +
                        '<span class="lu-aq-label">' + td.label + '</span>' +
                        '<span class="lu-aq-bar"><span class="lu-aq-bar-fill" style="width:' + barW + '%;background:' + td.color + '"></span></span>' +
                        '<span class="lu-aq-count">' + cnt + '</span>' +
                    '</div>';
            }
            if (fwtRows) {
                followingTiersHtml =
                    '<div class="lu-audience-quality">' +
                        '<div class="lu-aq-header" data-aq-toggle="lu-aq-following">' +
                            '<div class="lu-aq-title">Following Quality <span class="lu-sig-arrow">\u25B8</span></div>' +
                            '<div class="lu-aq-sub">' + fmtK(following) + ' following</div>' +
                        '</div>' +
                        '<div class="lu-aq-body" id="lu-aq-following" style="display:none">' + fwtRows + '</div>' +
                    '</div>';
            }
        }

        var reachCol =
            '<div class="lu-signal-reach">' +
                '<div class="lu-reach-item">' +
                    '<span class="lu-reach-num">' + fmtK(followers) + '</span>' +
                    '<span class="lu-reach-label">followers</span>' +
                '</div>' +
                '<div class="lu-reach-item">' +
                    '<span class="lu-reach-num">' + fmtK(following) + '</span>' +
                    '<span class="lu-reach-label">following</span>' +
                '</div>' +
                followerTiersHtml +
                followingTiersHtml +
            '</div>';

        // Right col: context signals
        var signalLists = [
            { key: 'muted_by_list', label: 'Muted by', count: mutedBy, warn: true },
            { key: 'reported_by_list', label: 'Reported by', count: reportedBy, warn: true },
            { key: 'muting_list', label: 'Muting', count: muting, warn: false },
            { key: 'reporting_list', label: 'Reporting', count: reporting, warn: false }
        ];

        var signalRows = '';
        for (var i = 0; i < signalLists.length; i++) {
            var s = signalLists[i];
            var people = data[s.key] || [];
            var total = data[s.key + '_total'] || s.count;
            var warnCls = s.warn && s.count > 0 ? ' lu-sig-warn-row' : '';
            if (s.warn && s.count >= 10) warnCls += ' lu-sig-high';
            var peopleHtml = '';
            if (people.length > 0) {
                peopleHtml = '<div class="lu-sig-people" style="display:none">';
                for (var j = 0; j < people.length; j++) {
                    var pp = people[j];
                    var ppNpub = pp.npub || '';
                    var ppLink = ppNpub ? 'https://ditto.pub/' + esc(ppNpub) : '';
                    peopleHtml += '<div class="lu-sig-person">' +
                        (ppLink ? '<a href="' + ppLink + '" target="_blank" rel="noopener" class="lu-sig-person-link">' : '') +
                        miniAvatar(pp.picture, pp.name, pp.pubkey, 18) +
                        ' <span>' + esc(pp.name || (pp.pubkey || '').slice(0, 12) + '...') + '</span>' +
                        (ppLink ? '</a>' : '') +
                    '</div>';
                }
                if (total > people.length) {
                    peopleHtml += '<button type="button" class="lu-sig-loadmore" data-sig-type="' + esc(s.key) + '" data-page="2" data-total="' + total + '">View all ' + total + ' \u2192</button>';
                }
                peopleHtml += '</div>';
            }
            var barHtml = '';
            if (s.warn && s.count > 0) {
                var pct = Math.min(s.count / 50 * 100, 100);
                barHtml = '<div class="lu-sig-bar"><div class="lu-sig-bar-fill" style="width:' + pct + '%"></div></div>';
            }
            signalRows +=
                '<div class="lu-sig-row2' + warnCls + '" data-expandable="' + (people.length > 0 ? '1' : '0') + '">' +
                    '<span class="lu-sig-num2' + (s.warn && s.count > 0 ? ' lu-sig-num-warn' : '') + '">' + s.count + '</span>' +
                    '<span class="lu-sig-label2">' + esc(s.label) + (people.length > 0 ? ' <span class="lu-sig-arrow">\u25B8</span>' : '') + '</span>' +
                    barHtml +
                    peopleHtml +
                '</div>';
        }

        var contextCol = '<div class="lu-signal-context">' + signalRows + '</div>';

        var zone3Html =
            '<div class="lu-zone lu-zone-signals">' +
                reachCol +
                '<div class="lu-signal-divider"></div>' +
                contextCol +
            '</div>';

        // Actions
        var actionsHtml =
            '<div class="lu-zone lu-zone-actions">' +
                '<a href="https://ditto.pub/' + esc(npub) + '" target="_blank" rel="noopener" class="lu-btn lu-btn-sm lu-btn-primary">Open Profile</a>' +
                (isMember && cardUrl ? '<a href="' + esc(cardUrl) + '" class="lu-btn lu-btn-sm">Member Card</a>' : '') +
                '<button type="button" class="lu-btn lu-btn-sm" id="lu-new-search">New Search</button>' +
            '</div>';

        // Assemble card — dossier layout with inner wrapper
        var html =
            '<div class="lu-card lu-card-enter" style="--lu-glow:' + glowColor + '">' +
            '<div class="lu-card-inner">' +
                // Zone 1: Identity + Trust
                '<div class="lu-zone lu-zone-identity">' +
                    '<div class="lu-z1-left">' +
                        '<div class="lu-avatar">' + avatarHtml + '</div>' +
                        '<div class="lu-identity">' +
                            nameHtml +
                            (nip05 ? '<div class="lu-nip05">' + esc(nip05) + '</div>' : '') +
                            '<div class="lu-npub">' + esc(truncNpub(npub)) + ' <button type="button" class="lu-copy" id="lu-copy-npub" title="Copy npub">\u2398</button></div>' +
                        '</div>' +
                    '</div>' +
                    trustRingHtml +
                '</div>' +
                pathHtml +
                mutualHtml +
                bioHtml +
                // Zone 2: Connection
                zone2Html +
                // Activity
                activityZoneHtml +
                // Zone 3: Signal dashboard
                zone3Html +
                // Actions
                actionsHtml +
            '</div>' +
            '</div>';

        result.innerHTML = html;
        result.style.display = '';

        // Staggered entrance
        var inner = result.querySelector('.lu-card-inner');
        if (inner) {
            var kids = inner.children;
            for (var ci = 0; ci < kids.length; ci++) {
                kids[ci].style.setProperty('--d', (0.05 + ci * 0.08) + 's');
            }
        }

        // Animate trust ring arc
        requestAnimationFrame(function() {
            var arc = result.querySelector('.lu-ring-arc');
            if (arc && arc.dataset.targetDash) {
                requestAnimationFrame(function() { arc.setAttribute('stroke-dasharray', arc.dataset.targetDash); });
            }
        });

        // Auto-retry trust if computing (single global timer, no re-creation on render)
        var noTrustPath = !data.computed_at && !(data.shared_follows_count > 0) && !(data.mutual_followers_count > 0) && _trustComputedOnce;
        if (!hasTrust && !isSelf && !noTrustPath && isConnected() && _lastLookupTarget && !_trustPollActive && _trustPollRetries < 6) {
            _trustPollActive = true;
            (function scheduleNextPoll() {
                _trustPollTimer = setTimeout(function() {
                    _trustPollRetries++;
                    if (_trustPollRetries >= 6 || !_lastLookupTarget) {
                        _trustPollActive = false;
                        return;
                    }
                    fetch('/api/directory/trust-lookup?target=' + encodeURIComponent(_lastLookupTarget) + '&observer=' + encodeURIComponent(connected.npub))
                        .then(function(r) { return r.ok ? r.json() : null; })
                        .then(function(d) {
                            if (!d) { scheduleNextPoll(); return; }
                            var hasIt = d.trust_score > 0 || (d.trust_tier && d.trust_tier !== 'unknown');
                            if (hasIt) {
                                _trustPollActive = false;
                                renderResult(d);
                            } else {
                                scheduleNextPoll();
                            }
                        })
                        .catch(function() { scheduleNextPoll(); });
                }, 5000);
            })();
        }

        // Hide/show CTA
        var dirCta = document.querySelector('.dir-cta');
        if (dirCta) dirCta.style.display = isMember ? 'none' : '';

        // Wire interactions
        document.getElementById('lu-new-search').onclick = function() {
            _lastLookupTarget = '';
            input.value = ''; input.focus(); result.style.display = 'none';
            var cta = document.querySelector('.dir-cta');
            if (cta && !connected.isMember) cta.style.display = '';
        };

        var cp = document.getElementById('lu-copy-npub');
        if (cp) cp.onclick = function() {
            if (navigator.clipboard) navigator.clipboard.writeText(npub).catch(function(){});
            cp.textContent = '\u2713';
            setTimeout(function() { cp.textContent = '\u2398'; }, 1500);
        };

        // Locked trust — connect button
        var lockedBtn = document.getElementById('lu-locked-connect');
        if (lockedBtn) {
            lockedBtn.onclick = function() {
                if (hasNip07()) {
                    doExtensionConnect();
                } else {
                    var inp = document.getElementById('wot-npub-input');
                    if (inp) inp.focus(); else window.scrollTo({ top: 0, behavior: 'smooth' });
                }
            };
        }

        // Expandable signal rows
        result.querySelectorAll('[data-expandable="1"]').forEach(function(row) {
            row.style.cursor = 'pointer';
            row.onclick = function(e) {
                if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON') return;
                var pp = row.querySelector('.lu-sig-people');
                var arrow = row.querySelector('.lu-sig-arrow');
                if (pp) {
                    var open = pp.style.display !== 'none';
                    pp.style.display = open ? 'none' : '';
                    if (arrow) arrow.textContent = open ? '\u25B8' : '\u25BE';
                }
            };
        });

        // "View all" / load more buttons for signal lists
        result.querySelectorAll('.lu-sig-loadmore').forEach(function(btn) {
            btn.onclick = function(e) {
                e.stopPropagation();
                var sigType = btn.dataset.sigType;
                var page = parseInt(btn.dataset.page, 10);
                var total = parseInt(btn.dataset.total, 10);
                btn.textContent = 'Loading...';
                btn.disabled = true;
                var targetParam = encodeURIComponent(data.npub || data.hex_pubkey || _lastLookupTarget);
                var observerParam = isConnected() ? ('&observer=' + encodeURIComponent(connected.npub)) : '';
                fetch('/api/directory/trust-lookup/signals?target=' + targetParam + '&type=' + sigType + '&page=' + page + '&limit=20' + observerParam)
                    .then(function(r) { return r.ok ? r.json() : null; })
                    .then(function(d) {
                        if (!d || !d.items || !d.items.length) {
                            btn.remove();
                            return;
                        }
                        var container = btn.parentNode;
                        d.items.forEach(function(pp) {
                            var el = document.createElement('div');
                            el.className = 'lu-sig-person';
                            var ppNpub = pp.npub || '';
                            var ppLink = ppNpub ? 'https://ditto.pub/' + esc(ppNpub) : '';
                            el.innerHTML =
                                (ppLink ? '<a href="' + ppLink + '" target="_blank" rel="noopener" class="lu-sig-person-link">' : '') +
                                miniAvatar(pp.picture, pp.name, pp.pubkey, 18) +
                                ' <span>' + esc(pp.name || (pp.pubkey || '').slice(0, 12) + '...') + '</span>' +
                                (ppLink ? '</a>' : '');
                            container.insertBefore(el, btn);
                        });
                        if (d.has_more) {
                            btn.dataset.page = page + 1;
                            var loaded = (page * 20);
                            btn.textContent = 'Load more (' + loaded + '/' + total + ') \u2192';
                            btn.disabled = false;
                        } else {
                            btn.remove();
                        }
                    })
                    .catch(function() {
                        btn.textContent = 'Error — try again';
                        btn.disabled = false;
                    });
            };
        });

        // Connection group toggles
        result.querySelectorAll('.lu-conn-header[data-toggle]').forEach(function(hdr) {
            hdr.style.cursor = 'pointer';
            hdr.onclick = function() {
                var listEl = document.getElementById(hdr.dataset.toggle);
                var arrow = hdr.querySelector('.lu-sig-arrow');
                if (listEl) {
                    var open = listEl.style.display !== 'none';
                    listEl.style.display = open ? 'none' : '';
                    if (arrow) arrow.textContent = open ? '\u25B8' : '\u25BE';
                }
            };
        });

        // Audience quality toggles
        result.querySelectorAll('.lu-aq-header[data-aq-toggle]').forEach(function(hdr) {
            hdr.style.cursor = 'pointer';
            hdr.onclick = function() {
                var body = document.getElementById(hdr.dataset.aqToggle);
                var arrow = hdr.querySelector('.lu-sig-arrow');
                if (body) {
                    var open = body.style.display !== 'none';
                    body.style.display = open ? 'none' : '';
                    if (arrow) arrow.textContent = open ? '\u25B8' : '\u25BE';
                }
            };
        });

        // People list filter inputs
        result.querySelectorAll('.lu-people-filter').forEach(function(filterInput) {
            filterInput.onclick = function(e) { e.stopPropagation(); };
            filterInput.oninput = function() {
                var q = filterInput.value.toLowerCase();
                var rows = filterInput.closest('.lu-people-list').querySelectorAll('.lu-person-row');
                rows.forEach(function(row) {
                    row.style.display = (!q || row.dataset.name.indexOf(q) >= 0) ? '' : 'none';
                });
            };
        });
    }

    // --- Discover people ---
    function loadDiscover() {
        if (!isConnected()) return;
        result.style.display = '';
        result.innerHTML =
            '<div class="lu-scanning">' +
                '<div class="lu-radar">' +
                    '<div class="lu-radar-circle r1"></div><div class="lu-radar-circle r2"></div><div class="lu-radar-circle r3"></div>' +
                    '<div class="lu-radar-sweep"></div>' +
                    '<div class="lu-radar-dot"></div>' +
                    '<div class="lu-radar-blip"></div><div class="lu-radar-blip"></div><div class="lu-radar-blip"></div>' +
                '</div>' +
                '<div class="lu-scan-text">Scanning relays\u2026</div>' +
            '</div>';

        fetch('/api/directory/recommendations/' + encodeURIComponent(connected.npub))
            .then(function(r) {
                if (!r.ok) return r.json().then(function(d) { throw new Error(d.detail || 'Failed'); });
                return r.json();
            })
            .then(renderDiscover)
            .catch(function(err) {
                result.innerHTML = '<div class="lu-error">' + esc(err.message) + '</div>';
            });
    }

    var _discoverData = null;

    function renderDiscover(data) {
        _discoverData = data;
        _renderDiscoverFiltered(0);
    }

    function _renderDiscoverFiltered(maxHops) {
        var data = _discoverData;
        if (!data) return;
        var recs = data.recommendations || [];

        // Filter by hops if set
        var filtered = maxHops > 0
            ? recs.filter(function(r) { return r.hops != null && r.hops <= maxHops; })
            : recs;

        // Build hops filter buttons
        var hopsValues = {};
        for (var i = 0; i < recs.length; i++) {
            var h = recs[i].hops;
            if (h != null && h > 0) hopsValues[h] = (hopsValues[h] || 0) + 1;
        }
        var hopKeys = Object.keys(hopsValues).map(Number).sort();
        var hopsFilterHtml = '';
        if (hopKeys.length > 0) {
            hopsFilterHtml = '<div class="wot-disc-hops">' +
                '<span class="wot-disc-hops-label">Filter by hops:</span>' +
                '<button type="button" class="wot-disc-hops-btn' + (maxHops === 0 ? ' active' : '') + '" data-hops="0">All</button>';
            for (var hi = 0; hi < hopKeys.length; hi++) {
                var hv = hopKeys[hi];
                hopsFilterHtml += '<button type="button" class="wot-disc-hops-btn' + (maxHops === hv ? ' active' : '') + '" data-hops="' + hv + '">' + hv + ' hop' + (hv !== 1 ? 's' : '') + ' (' + hopsValues[hv] + ')</button>';
            }
            hopsFilterHtml += '</div>';
        }

        if (filtered.length === 0 && recs.length === 0) {
            result.innerHTML =
                '<div class="lu-not-found">' +
                    '<div class="lu-nf-title">No recommendations yet</div>' +
                    '<p class="lu-nf-desc">Follow more people using a Nostr client to build your network.</p>' +
                    '<button type="button" class="lu-btn" id="lu-back-dir">Back to Directory</button>' +
                '</div>';
            document.getElementById('lu-back-dir').onclick = function() { result.style.display = 'none'; };
            return;
        }

        var cardsHtml = '';
        for (var i = 0; i < filtered.length; i++) {
            var r = filtered[i];
            var pct = Math.round((r.trust_score || 0) * 100);
            var tierColor = pct >= 70 ? '#F7931A' : pct >= 40 ? '#c084fc' : pct >= 15 ? '#a78bfa' : pct >= 2 ? '#7c3aed' : '#4c1d95';
            var tierLabel = pct >= 70 ? 'Highly Trusted' : pct >= 40 ? 'Trusted' : pct >= 15 ? 'Neutral' : pct >= 2 ? 'Low Trust' : 'Unverified';

            var mutualText = '';
            if (r.mutual_follow_names && r.mutual_follow_names.length > 0) {
                mutualText = 'Followed by ' + r.mutual_follow_names.join(', ');
                var extra = (r.mutual_follow_count || 0) - r.mutual_follow_names.length;
                if (extra > 0) mutualText += ' +' + extra + ' you trust';
            }

            var hopsTag = r.hops != null ? '<span class="wot-disc-hops-tag">' + r.hops + ' hop' + (r.hops !== 1 ? 's' : '') + '</span>' : '';

            cardsHtml +=
                '<div class="wot-disc-card" style="border-color:' + tierColor + '25">' +
                    '<div class="wot-disc-top">' +
                        miniAvatar(r.picture, r.display_name, r.hex_pubkey, 36) +
                        '<div class="wot-disc-info">' +
                            '<div class="wot-disc-name">' + esc(r.display_name || 'Anonymous') + hopsTag + '</div>' +
                            (mutualText ? '<div class="wot-disc-mutual">' + esc(mutualText) + '</div>' : '') +
                        '</div>' +
                        '<button type="button" class="lu-btn lu-btn-sm wot-disc-view" data-npub="' + esc(r.npub) + '">View</button>' +
                    '</div>' +
                    '<div class="wot-disc-bar-wrap">' +
                        '<div class="wot-disc-bar-track"><div class="wot-disc-bar-fill" style="width:' + pct + '%;background:' + tierColor + '"></div></div>' +
                        '<span class="wot-disc-bar-pct" style="color:' + tierColor + '">' + pct + '% ' + esc(tierLabel) + '</span>' +
                    '</div>' +
                '</div>';
        }

        var countText = maxHops > 0
            ? '<span class="wot-disc-count">' + filtered.length + ' of ' + recs.length + ' people within ' + maxHops + ' hop' + (maxHops !== 1 ? 's' : '') + '</span>'
            : '';

        result.innerHTML =
            '<div class="wot-discover">' +
                '<div class="wot-disc-title">Discover People</div>' +
                '<p class="wot-disc-sub">Ranked by <a href="https://github.com/NosFabrica/brainstorm_graperank_algorithm" target="_blank" rel="noopener" style="color:var(--neon-purple)">GrapeRank</a> \u2014 a personalized trust algorithm that walks your follow graph to find who your network trusts most.</p>' +
                hopsFilterHtml +
                countText +
                (filtered.length > 0 ? cardsHtml : '<p class="wot-disc-hint" style="margin:1.5rem 0">No people found within ' + maxHops + ' hop' + (maxHops !== 1 ? 's' : '') + '. Try increasing the distance.</p>') +
                '<p class="wot-disc-hint">People you don\'t follow yet, scored by: mutual follows (50%), trust of mutual follows (30%), and their direct GrapeRank score from your perspective (20%).</p>' +
                '<button type="button" class="lu-btn" id="lu-back-dir">Back to Directory</button>' +
            '</div>';

        document.getElementById('lu-back-dir').onclick = function() { result.style.display = 'none'; };

        result.querySelectorAll('.wot-disc-view').forEach(function(b) {
            b.onclick = function() { input.value = b.dataset.npub; doSearch(); };
        });

        result.querySelectorAll('.wot-disc-hops-btn').forEach(function(b) {
            b.onclick = function() { _renderDiscoverFiltered(parseInt(b.dataset.hops, 10)); };
        });
    }

    // --- Init ---
    // Retry NIP-07 detection (some extensions inject late)
    var _nip07retries = 0;
    function retryNip07() {
        if (hasNip07() || _nip07retries > 10) return;
        _nip07retries++;
        setTimeout(function() {
            if (hasNip07() && !isConnected()) renderConnectArea();
            else retryNip07();
        }, 300);
    }

    if (isConnected()) {
        fetchMyProfile();
    } else {
        renderConnectArea();
        retryNip07();
    }

    input.addEventListener('keydown', function(e) { if (e.key === 'Enter') { e.preventDefault(); doSearch(); } });
    btn.addEventListener('click', doSearch);
})();
