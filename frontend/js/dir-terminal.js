(function() {
    'use strict';

    var sortContainer = document.getElementById('dfilter-sort');
    var searchInput = document.getElementById('dfilter-search');
    var tagWrap = document.getElementById('dfilter-tag-wrap');
    var tagToggle = document.getElementById('dfilter-tag-toggle');
    var tagDropdown = document.getElementById('dfilter-tag-dropdown');
    var badgesBtns = document.querySelectorAll('.dfilter-badge-btn');
    var viewBtn = document.getElementById('dfilter-view');
    var activeWrap = document.getElementById('dfilter-active');
    if (!sortContainer || !searchInput) return;

    var state = {
        search: '',
        tags: [],
        sort: 'newest',
        badge: '',
        view: 'list'
    };

    var allTags = [];
    var searchTimer = null;

    window.__dirTerminal = {
        getState: function() { return state; },
        onChange: null,
        setSort: function(sort) {
            state.sort = sort;
            syncSortButtons();
            renderActive();
            fireChange();
        }
    };

    // -----------------------------------------------------------------------
    // URL sync
    // -----------------------------------------------------------------------
    function readURL() {
        var p = new URLSearchParams(location.search);
        state.sort = p.get('sort') || 'newest';
        state.badge = p.get('badge') || '';
        state.search = p.get('search') || '';
        var urlTags = p.get('tags');
        state.tags = urlTags ? urlTags.split(',').map(function(t) { return t.trim(); }).filter(Boolean) : [];
        state.view = localStorage.getItem('dir-view') || 'list';
        searchInput.value = state.search;
        syncSortButtons();
        syncBadgeButtons();
        syncViewBtn();
        renderActive();
    }

    function pushURL() {
        var p = new URLSearchParams();
        if (state.sort !== 'newest') p.set('sort', state.sort);
        if (state.badge) p.set('badge', state.badge);
        if (state.search) p.set('search', state.search);
        if (state.tags.length > 0) p.set('tags', state.tags.join(','));
        var qs = p.toString();
        history.replaceState(null, '', '/directory' + (qs ? '?' + qs : ''));
    }

    function fireChange() {
        pushURL();
        if (window.__dirTerminal.onChange) window.__dirTerminal.onChange(state);
    }

    function esc(s) {
        var d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    // -----------------------------------------------------------------------
    // Sort
    // -----------------------------------------------------------------------
    var sortSlider = sortContainer.querySelector('.dfilter-sort-slider');

    function positionSortSlider() {
        if (!sortSlider) return;
        var activeBtn = sortContainer.querySelector('.dfilter-sort-btn.active');
        if (!activeBtn) { sortSlider.style.opacity = '0'; return; }
        var containerRect = sortContainer.getBoundingClientRect();
        var btnRect = activeBtn.getBoundingClientRect();
        sortSlider.style.width = btnRect.width + 'px';
        sortSlider.style.transform = 'translateX(' + (btnRect.left - containerRect.left - 3) + 'px)';
        sortSlider.style.opacity = '1';
    }

    function syncSortButtons() {
        var btns = sortContainer.querySelectorAll('.dfilter-sort-btn');
        for (var i = 0; i < btns.length; i++) {
            btns[i].classList.toggle('active', btns[i].dataset.sort === state.sort);
        }
        positionSortSlider();
    }

    sortContainer.addEventListener('click', function(e) {
        var btn = e.target.closest('.dfilter-sort-btn');
        if (!btn) return;
        state.sort = btn.dataset.sort;
        syncSortButtons();
        renderActive();
        fireChange();
    });

    // -----------------------------------------------------------------------
    // Search (debounced)
    // -----------------------------------------------------------------------
    searchInput.addEventListener('input', function() {
        clearTimeout(searchTimer);
        var val = this.value.trim();
        searchTimer = setTimeout(function() {
            state.search = val;
            renderActive();
            fireChange();
        }, 400);
    });

    searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            clearTimeout(searchTimer);
            state.search = this.value.trim();
            renderActive();
            fireChange();
        }
    });

    // -----------------------------------------------------------------------
    // Tag dropdown
    // -----------------------------------------------------------------------
    function renderTagDropdown(filter) {
        var q = (filter || '').toLowerCase();
        var filtered = allTags.filter(function(t) {
            return !q || t.name.toLowerCase().indexOf(q) !== -1;
        });
        var html = '<input type="text" class="dfilter-tag-search" placeholder="Search tags..." autocomplete="off" spellcheck="false">';
        if (filtered.length === 0) {
            html += '<div class="dfilter-tag-empty">No tags found</div>';
        } else {
            html += filtered.map(function(t) {
                var isActive = state.tags.indexOf(t.name) !== -1;
                return '<div class="dfilter-tag-option' + (isActive ? ' active' : '') + '" data-tag="' + esc(t.name) + '">' +
                    '<span>' + esc(t.name) + '</span>' +
                    '<span class="tag-count">' + t.count + '</span>' +
                '</div>';
            }).join('');
        }
        tagDropdown.innerHTML = html;
        var searchEl = tagDropdown.querySelector('.dfilter-tag-search');
        if (searchEl) {
            if (q) searchEl.value = q;
            searchEl.addEventListener('input', function() {
                var val = this.value;
                renderTagDropdown(val);
                // Re-focus and restore cursor
                var s = tagDropdown.querySelector('.dfilter-tag-search');
                if (s) { s.focus(); s.value = val; }
            });
            searchEl.addEventListener('click', function(e) { e.stopPropagation(); });
        }
    }

    function updateTagToggleLabel() {
        var count = state.tags.length;
        tagToggle.innerHTML = (count > 0 ? 'Tags (' + count + ')' : 'Tags') +
            ' <svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M6 9l6 6 6-6"/></svg>';
    }

    tagToggle.addEventListener('click', function(e) {
        e.stopPropagation();
        var isOpen = tagDropdown.classList.contains('open');
        if (isOpen) {
            tagDropdown.classList.remove('open');
            tagToggle.classList.remove('open');
        } else {
            renderTagDropdown('');
            tagDropdown.classList.add('open');
            tagToggle.classList.add('open');
            var s = tagDropdown.querySelector('.dfilter-tag-search');
            if (s) s.focus();
        }
    });

    tagDropdown.addEventListener('click', function(e) {
        var opt = e.target.closest('.dfilter-tag-option');
        if (!opt) return;
        e.stopPropagation();
        var tag = opt.dataset.tag;
        var idx = state.tags.indexOf(tag);
        if (idx === -1) {
            state.tags.push(tag);
        } else {
            state.tags.splice(idx, 1);
        }
        opt.classList.toggle('active');
        updateTagToggleLabel();
        renderActive();
        fireChange();
    });

    // Close dropdown on outside click
    document.addEventListener('click', function(e) {
        if (!e.target.closest('#dfilter-tag-wrap')) {
            tagDropdown.classList.remove('open');
            tagToggle.classList.remove('open');
        }
    });

    // Fetch tags
    var tagPromise = window.__dirPrefetch && window.__dirPrefetch.tags
        ? window.__dirPrefetch.tags
        : fetch(location.origin + '/api/directory/tags').then(function(r) { return r.json(); });
    tagPromise.then(function(data) {
        allTags = data || [];
        if (allTags.length > 0) tagWrap.style.display = '';
        readURL();
        updateTagToggleLabel();
        fireChange();
        requestAnimationFrame(positionSortSlider);
    }).catch(function() { readURL(); fireChange(); requestAnimationFrame(positionSortSlider); });

    window.addEventListener('resize', positionSortSlider);

    // -----------------------------------------------------------------------
    // Badge buttons
    // -----------------------------------------------------------------------
    function syncBadgeButtons() {
        for (var i = 0; i < badgesBtns.length; i++) {
            badgesBtns[i].classList.toggle('active', badgesBtns[i].dataset.badge === state.badge);
        }
    }

    for (var bi = 0; bi < badgesBtns.length; bi++) {
        badgesBtns[bi].addEventListener('click', function() {
            state.badge = state.badge === this.dataset.badge ? '' : this.dataset.badge;
            syncBadgeButtons();
            renderActive();
            fireChange();
        });
    }

    // -----------------------------------------------------------------------
    // View toggle
    // -----------------------------------------------------------------------
    function syncViewBtn() {
        if (!viewBtn) return;
        viewBtn.classList.toggle('grid-active', state.view === 'grid');
        viewBtn.title = state.view === 'grid' ? 'Switch to list' : 'Switch to grid';
    }

    if (viewBtn) {
        viewBtn.addEventListener('click', function() {
            state.view = state.view === 'grid' ? 'list' : 'grid';
            localStorage.setItem('dir-view', state.view);
            syncViewBtn();
            fireChange();
        });
    }

    // -----------------------------------------------------------------------
    // Active filter chips
    // -----------------------------------------------------------------------
    function renderActive() {
        if (!activeWrap) return;
        var chips = [];

        if (state.search) {
            chips.push('<span class="dfilter-chip">search: ' + esc(state.search) + '<span class="dfilter-chip-x" data-action="clear-search">&times;</span></span>');
        }
        state.tags.forEach(function(t) {
            chips.push('<span class="dfilter-chip">tag: ' + esc(t) + '<span class="dfilter-chip-x" data-action="remove-tag" data-tag="' + esc(t) + '">&times;</span></span>');
        });
        if (state.badge) {
            chips.push('<span class="dfilter-chip">badge: ' + esc(state.badge.replace(/-/g, ' ')) + '<span class="dfilter-chip-x" data-action="clear-badge">&times;</span></span>');
        }
        if (state.sort !== 'newest') {
            chips.push('<span class="dfilter-chip">sort: ' + esc(state.sort) + '<span class="dfilter-chip-x" data-action="clear-sort">&times;</span></span>');
        }

        if (chips.length > 0) {
            chips.push('<span class="dfilter-chip dfilter-chip-reset" data-action="reset">clear all</span>');
            activeWrap.innerHTML = chips.join('');
            activeWrap.style.display = '';
        } else {
            activeWrap.innerHTML = '';
            activeWrap.style.display = 'none';
        }
    }

    activeWrap.addEventListener('click', function(e) {
        var el = e.target.closest('[data-action]');
        if (!el) return;
        var a = el.dataset.action;
        if (a === 'clear-search') { state.search = ''; searchInput.value = ''; }
        else if (a === 'remove-tag') {
            state.tags = state.tags.filter(function(t) { return t !== el.dataset.tag; });
            updateTagToggleLabel();
        }
        else if (a === 'clear-badge') { state.badge = ''; syncBadgeButtons(); }
        else if (a === 'clear-sort') { state.sort = 'newest'; syncSortButtons(); }
        else if (a === 'reset') {
            state.search = ''; state.tags = []; state.sort = 'newest'; state.badge = '';
            searchInput.value = '';
            syncSortButtons();
            syncBadgeButtons();
            updateTagToggleLabel();
        }
        renderActive();
        fireChange();
    });
})();
