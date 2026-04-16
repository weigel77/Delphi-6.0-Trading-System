(function () {
    const payloadElement = document.getElementById('hosted-shell-initial-payload');
    const viewRoot = document.querySelector('[data-hosted-view]');
    const refreshButton = document.getElementById('hosted-shell-refresh-button');
    const runButton = document.getElementById('hosted-shell-run-button');
    const syncBadge = document.getElementById('hosted-shell-sync-badge');
    const syncDetail = document.getElementById('hosted-shell-sync-detail');

    if (!viewRoot || !payloadElement) {
        return;
    }

    const escapeHtml = (value) => String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');

    const readInitialPayload = () => {
        try {
            return JSON.parse(payloadElement.textContent || '{}');
        } catch {
            return {};
        }
    };

    const setSyncState = (badge, detail) => {
        if (syncBadge) {
            syncBadge.textContent = badge;
        }
        if (syncDetail) {
            syncDetail.textContent = detail;
        }
    };

    const formatNumber = (value, suffix = '') => `${Number(value || 0).toFixed(2)}${suffix}`;

    const setErrorState = (message) => {
        const viewName = viewRoot.dataset.hostedView || '';
        const block = document.getElementById(`hosted-${viewName}-error-block`);
        const text = document.getElementById(`hosted-${viewName}-error-text`);
        if (block) {
            block.hidden = !message;
        }
        if (text) {
            text.textContent = message || '';
        }
    };

    const renderPerformance = (response) => {
        const payload = response.payload || {};
        const metrics = payload.metrics || {};
        const totals = metrics.totals || {};
        const learningOverview = payload.learning?.overview || {};
        const filterList = document.getElementById('hosted-performance-filter-list');
        const outcomeGrid = document.getElementById('hosted-performance-outcome-grid');

        const setText = (id, value) => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = value;
            }
        };

        setText('hosted-performance-records-caption', `${payload.records_filtered || 0} filtered of ${payload.records_total || 0} total records`);
        setText('hosted-performance-total-trades', totals.total_trades || 0);
        setText('hosted-performance-open-trades', totals.open_trades || 0);
        setText('hosted-performance-closed-trades', totals.closed_trades || 0);
        setText('hosted-performance-win-rate', `${formatNumber(metrics.win_rate?.value, '%')}`);
        setText('hosted-performance-expectancy', formatNumber(metrics.expectancy?.value));
        setText('hosted-performance-net-pnl', formatNumber(metrics.net_pnl?.value));
        setText('hosted-performance-safety-ratio', formatNumber(learningOverview.avg_safety_ratio));
        setText('hosted-performance-credit-efficiency', `${formatNumber(metrics.credit_efficiency?.value, '%')}`);

        if (filterList) {
            filterList.innerHTML = Object.entries(payload.filters || {})
                .map(([group, values]) => `<li>${escapeHtml(group.replaceAll('_', ' '))}: ${escapeHtml((values || []).join(', ') || 'none')}</li>`)
                .join('');
        }

        if (outcomeGrid) {
            outcomeGrid.innerHTML = [
                ['Wins', totals.wins || 0],
                ['Loss Outcomes', totals.loss_outcomes || 0],
                ['Black Swans', totals.black_swan_count || 0],
                ['Scratched', totals.scratched_count || 0],
            ].map(([label, value]) => `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`).join('');
        }
    };

    const renderJournal = (response) => {
        const summaryGrid = document.getElementById('hosted-journal-summary-grid');
        const tableBody = document.getElementById('hosted-journal-table-body');
        if (summaryGrid) {
            summaryGrid.innerHTML = (response.summary_metrics || []).map((item) => `
                <article class="hosted-shell-summary-card">
                    <p>${escapeHtml(item.label)}</p>
                    <strong>${escapeHtml(item.value)}</strong>
                </article>
            `).join('');
        }
        if (tableBody) {
            const rows = response.trades || [];
            tableBody.innerHTML = rows.length ? rows.map((trade) => `
                <tr>
                    <td>${escapeHtml(trade.trade_number)}</td>
                    <td>${escapeHtml(trade.status)}</td>
                    <td>${escapeHtml(trade.candidate_profile)}</td>
                    <td>${escapeHtml(trade.system_name)}</td>
                    <td>${escapeHtml(trade.trade_date)}</td>
                    <td>${escapeHtml(trade.expiration_date)}</td>
                    <td>${escapeHtml(trade.strike_pair)}</td>
                    <td>${escapeHtml(trade.contracts)}</td>
                    <td>${escapeHtml(trade.gross_pnl)}</td>
                    <td>${escapeHtml(trade.win_loss_result)}</td>
                </tr>
            `).join('') : '<tr><td colspan="10" class="hosted-shell-empty-cell">No trades were returned for this hosted journal view.</td></tr>';
        }
    };

    const renderOpenTrades = (response) => {
        const setText = (id, value) => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = value;
            }
        };
        setText('hosted-open-open-count', response.open_trade_count || 0);
        setText('hosted-open-evaluated-at', response.evaluated_at_display || '—');
        setText('hosted-open-notifications', response.notifications_enabled ? 'ON' : 'OFF');
        setText('hosted-open-alerts-sent', response.alerts_sent || 0);

        const statusCounts = document.getElementById('hosted-open-status-counts');
        if (statusCounts) {
            const items = response.status_counts || [];
            statusCounts.innerHTML = items.length ? items.map((item) => `<span>${escapeHtml(item.status)}: ${escapeHtml(item.count)}</span>`).join('') : '<span>No open-trade statuses returned.</span>';
        }

        const tableBody = document.getElementById('hosted-open-table-body');
        if (tableBody) {
            const rows = response.records || [];
            tableBody.innerHTML = rows.length ? rows.map((item) => `
                <tr>
                    <td>${escapeHtml(item.trade_number || item.trade_id || '')}</td>
                    <td>${escapeHtml(item.trade_mode || '—')}</td>
                    <td>${escapeHtml(item.system_name || '—')}</td>
                    <td>${escapeHtml(item.profile_label || '—')}</td>
                    <td>${escapeHtml(item.status || '—')}</td>
                    <td>${escapeHtml(item.current_em_multiple_display || item.current_em_multiple || '—')}</td>
                    <td>${escapeHtml(item.current_spread_mark_display || item.current_spread_mark || '—')}</td>
                    <td>${escapeHtml(item.current_total_close_cost_display || '—')}</td>
                    <td>${escapeHtml(item.action_recommendation || 'Review')}</td>
                    <td>${escapeHtml(item.next_trigger || '—')}</td>
                </tr>
            `).join('') : '<tr><td colspan="10" class="hosted-shell-empty-cell">No open trades were returned for this hosted filter.</td></tr>';
        }
    };

    const renderManageTrades = (response) => {
        const setText = (id, value) => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = value;
            }
        };
        setText('hosted-manage-open-count', response.open_trade_count || 0);
        setText('hosted-manage-evaluated-at', response.evaluated_at_display || '—');
        setText('hosted-manage-notifications', response.notifications_enabled ? 'ON' : 'OFF');
        setText('hosted-manage-records-filtered', response.records_filtered || 0);

        const statusCounts = document.getElementById('hosted-manage-status-counts');
        if (statusCounts) {
            const items = response.status_counts || [];
            statusCounts.innerHTML = items.length ? items.map((item) => `<span>${escapeHtml(item.status)}: ${escapeHtml(item.count)}</span>`).join('') : '<span>No active management statuses returned.</span>';
        }

        const tableBody = document.getElementById('hosted-manage-table-body');
        if (tableBody) {
            const rows = (response.records || []).slice(0, 8);
            tableBody.innerHTML = rows.length ? rows.map((item) => `
                <tr>
                    <td>${escapeHtml(item.trade_number || item.trade_id || '')}</td>
                    <td>${escapeHtml(item.trade_mode || '—')}</td>
                    <td>${escapeHtml(item.status || '—')}</td>
                    <td>${escapeHtml(item.action_recommendation || 'Review')}</td>
                    <td>${escapeHtml(item.current_total_close_cost_display || '—')}</td>
                    <td>${escapeHtml(item.alert_state?.last_alert_type || 'Not sent')}</td>
                </tr>
            `).join('') : '<tr><td colspan="6" class="hosted-shell-empty-cell">No hosted management records are available yet.</td></tr>';
        }
    };

    const renderApollo = (response) => {
        const payload = response.payload || {};
        const setText = (id, value) => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = value;
            }
        };

        setText('hosted-apollo-status', payload.status || 'No Snapshot');
        setText('hosted-apollo-run-timestamp', payload.run_timestamp || 'Not captured yet');
        setText('hosted-apollo-execution-source', payload.execution_source_label || 'Live data');
        setText('hosted-apollo-live-provider', payload.live_data_provider || payload.provider_name || '—');
        setText('hosted-apollo-structure-grade', payload.structure_grade || '—');
        setText('hosted-apollo-macro-grade', payload.macro_grade || '—');
        setText('hosted-apollo-candidate-count', payload.trade_candidates_valid_count || 0);
        setText('hosted-apollo-next-market-day', payload.next_market_day || '—');
        setText('hosted-apollo-provider', payload.provider_name || '—');
        setText('hosted-apollo-option-chain-status', payload.option_chain_status || 'Unavailable');
        setText('hosted-apollo-spx-value', payload.spx_value || '—');
        setText('hosted-apollo-vix-value', payload.vix_value || '—');

        const emptyState = document.getElementById('hosted-apollo-empty-state');
        const emptyStateCopy = document.getElementById('hosted-apollo-empty-state-copy');
        if (emptyState) {
            emptyState.hidden = Boolean(response.snapshot_available);
        }
        if (emptyStateCopy && !response.snapshot_available) {
            emptyStateCopy.textContent = 'No Apollo run has been captured yet. Run Apollo locally to publish the latest hosted read-only snapshot.';
        }
        setErrorState('');

        const reasons = document.getElementById('hosted-apollo-reasons');
        if (reasons) {
            const items = payload.reasons || [];
            reasons.innerHTML = items.length
                ? items.map((item) => `<li>${escapeHtml(item)}</li>`).join('')
                : '<li>No Apollo notes were captured for this snapshot.</li>';
        }

        const tableBody = document.getElementById('hosted-apollo-candidate-table-body');
        if (tableBody) {
            const rows = payload.trade_candidates_items || [];
            tableBody.innerHTML = rows.length ? rows.map((item) => `
                <tr>
                    <td>${escapeHtml(item.mode_label || '')}</td>
                    <td>${escapeHtml(item.available ? 'Ready' : (item.no_trade_message || 'Stand Aside'))}</td>
                    <td>${escapeHtml(`${item.short_strike || '—'} / ${item.long_strike || '—'}`)}</td>
                    <td>${escapeHtml(item.net_credit || '—')}</td>
                    <td>${escapeHtml(item.max_loss || '—')}</td>
                    <td>${escapeHtml(item.em_multiple || '—')}</td>
                </tr>
            `).join('') : '<tr><td colspan="6" class="hosted-shell-empty-cell">No Apollo candidates are available in the captured snapshot.</td></tr>';
        }
    };

    const renderKairos = (response) => {
        const payload = response.payload || {};
        const setText = (id, value) => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = value;
            }
        };

        setText('hosted-kairos-session-status', payload.session_status || 'Inactive');
        setText('hosted-kairos-last-scan', payload.last_scan_display || '—');
        setText('hosted-kairos-execution-source', payload.execution_source_label || 'Hosted live execution');
        setText('hosted-kairos-live-provider', payload.live_data_provider || 'Schwab');
        setText('hosted-kairos-current-state', payload.current_state_display || 'Inactive');
        setText('hosted-kairos-structure-status', payload.structure_status || 'Awaiting scan');
        setText('hosted-kairos-window-found', payload.window_found ? 'Yes' : 'No');
        setText('hosted-kairos-total-scans', payload.total_scans_completed || 0);
        setText('hosted-kairos-spx-value', payload.spx_value || '—');
        setText('hosted-kairos-vix-value', payload.vix_value || '—');
        setText('hosted-kairos-momentum-status', payload.momentum_status || 'Awaiting scan');
        setText('hosted-kairos-timing-status', payload.timing_status || 'Awaiting scan');
        setText('hosted-kairos-market-session', payload.market_session_status || 'Closed');
        setText('hosted-kairos-next-scan', payload.next_scan_display || '—');
        setText('hosted-kairos-recommendation', payload.hosted_recommendation || 'Waiting for the next open market session');
        setText('hosted-kairos-summary-text', payload.hosted_market_note || payload.summary_text || 'No Kairos scan has completed yet.');
        setText('hosted-kairos-classification-note', payload.classification_note || '—');
        setErrorState('');

        const stamps = document.getElementById('hosted-kairos-stamps');
        if (stamps) {
            const items = payload.stamps || [];
            stamps.innerHTML = items.length
                ? items.map((item) => `<span>${escapeHtml(item.label || 'Stamp')}: ${escapeHtml(item.value || '—')}</span>`).join('')
                : '<span>No Kairos workspace stamps are available.</span>';
        }

        const lifecycle = document.getElementById('hosted-kairos-lifecycle-items');
        if (lifecycle) {
            const items = payload.lifecycle_items || [];
            lifecycle.innerHTML = items.length
                ? items.map((item) => `<li>${escapeHtml(item.label || 'Step')}: ${escapeHtml(item.value || '—')}</li>`).join('')
                : '<li>No Kairos lifecycle events are available.</li>';
        }

        const candidateCards = document.getElementById('hosted-kairos-candidate-cards');
        if (candidateCards) {
            const rows = payload.candidate_cards || [];
            candidateCards.innerHTML = rows.length ? rows.map((item) => `
                <article class="apollo-candidate-card apollo-candidate-card-${escapeHtml(item.mode_key || 'standard')} kairos-live-candidate-card${item.available ? '' : ' kairos-live-candidate-card-unavailable'}">
                    <div class="apollo-candidate-header">
                        <div class="apollo-candidate-mode">
                            <div>
                                <p class="apollo-candidate-rank">${escapeHtml(item.slot_label || 'Kairos')}</p>
                                <h4>${escapeHtml(item.headline || item.slot_label || 'Kairos Candidate')}</h4>
                                <p class="subtle">${escapeHtml(item.descriptor || item.message || '—')}</p>
                            </div>
                        </div>
                        <div class="apollo-candidate-header-meta">
                            <span class="kairos-runner-candidate-status kairos-runner-candidate-status-${item.tradeable ? 'qualified' : 'did-not-qualify'}">${escapeHtml(item.tradeable ? 'Ready' : (item.available ? 'Watch' : 'Unavailable'))}</span>
                            <p class="subtle">${escapeHtml(item.strike_label || '—')}</p>
                        </div>
                    </div>
                    <div class="apollo-candidate-stamp-row">
                        <div class="apollo-candidate-stamp apollo-candidate-stamp-credit"><span>Net Credit</span><strong>${escapeHtml(item.net_credit || '—')}</strong></div>
                        <div class="apollo-candidate-stamp"><span>Distance</span><strong>${escapeHtml(item.distance_to_short || '—')}</strong></div>
                        <div class="apollo-candidate-stamp"><span>EM Multiple</span><strong>${escapeHtml(item.em_multiple || '—')}</strong></div>
                    </div>
                    <p class="kairos-runner-candidate-rationale">${escapeHtml(item.message || '—')}</p>
                </article>
            `).join('') : '<div class="message message-info message-inline">No Kairos recommendation cards are available right now.</div>';
        }

        const tableBody = document.getElementById('hosted-kairos-candidate-table-body');
        if (tableBody) {
            const rows = payload.candidate_cards || [];
            tableBody.innerHTML = rows.length ? rows.map((item) => `
                <tr>
                    <td>${escapeHtml(item.slot_label || '')}</td>
                    <td>${escapeHtml(item.tradeable ? 'Tradeable' : (item.available ? 'Watch' : 'Unavailable'))}</td>
                    <td>${escapeHtml(item.strike_label || '—')}</td>
                    <td>${escapeHtml(item.net_credit || '—')}</td>
                    <td>${escapeHtml(item.distance_to_short || '—')}</td>
                    <td>${escapeHtml(item.em_multiple || '—')}</td>
                    <td>${escapeHtml(item.message || '—')}</td>
                </tr>
            `).join('') : '<tr><td colspan="7" class="hosted-shell-empty-cell">No Kairos recommendation cards are available.</td></tr>';
        }
    };

    const renderers = {
        performance: renderPerformance,
        journal: renderJournal,
        'open-trades': renderOpenTrades,
        'manage-trades': renderManageTrades,
        apollo: renderApollo,
        kairos: renderKairos,
    };

    const renderPayload = (payload) => {
        const renderer = renderers[viewRoot.dataset.hostedView || ''];
        if (renderer) {
            renderer(payload || {});
        }
    };

    renderPayload(readInitialPayload());

    const runRequest = async ({ button, url, method, busyText, busyBadge, busyDetail, successBadge }) => {
        if (!url) {
            return;
        }
        const originalText = button.textContent;
        button.disabled = true;
        button.textContent = busyText;
        setErrorState('');
        setSyncState(busyBadge, busyDetail);
        try {
            const response = await fetch(url, { method, headers: { 'Accept': 'application/json' } });
            const payload = await response.json();
            if (!response.ok || !payload.ok) {
                throw new Error(payload.detail || payload.error || 'Hosted action request failed.');
            }
            renderPayload(payload);
            setSyncState(successBadge, `Updated from ${url}`);
        } catch (error) {
            const message = error.message || 'Hosted action request failed.';
            setErrorState(message);
            setSyncState('Request failed', message);
        } finally {
            button.disabled = false;
            button.textContent = originalText;
        }
    };

    if (refreshButton) {
        refreshButton.addEventListener('click', async () => {
            await runRequest({
                button: refreshButton,
                url: refreshButton.dataset.hostedRefreshUrl || viewRoot.dataset.actionUrl,
                method: 'GET',
                busyText: 'Refreshing...',
                busyBadge: 'Refreshing',
                busyDetail: 'Requesting fresh data from the hosted action endpoint.',
                successBadge: 'Live synced',
            });
        });
    }

    if (runButton) {
        runButton.addEventListener('click', async () => {
            await runRequest({
                button: runButton,
                url: runButton.dataset.hostedRunUrl,
                method: 'POST',
                busyText: 'Running...',
                busyBadge: 'Running live',
                busyDetail: 'Executing the live engine against fresh Schwab data.',
                successBadge: 'Live execution complete',
            });
        });
    }
})();