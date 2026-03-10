/**
 * Data Analysis Agent — 前端交互逻辑
 */
(function () {
    'use strict';

    // DOM 元素
    const chatForm = document.getElementById('chat-form');
    const queryInput = document.getElementById('query-input');
    const chatMessages = document.getElementById('chat-messages');
    const sendBtn = document.getElementById('send-btn');
    const exampleBtns = document.querySelectorAll('.example-btn');

    // 状态
    let isLoading = false;

    // ====================== 初始化 ======================

    function init() {
        if (chatForm) chatForm.addEventListener('submit', handleSubmit);
        if (queryInput) queryInput.addEventListener('keydown', handleKeydown);

        exampleBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const query = btn.dataset.query || btn.textContent.trim();
                queryInput.value = query;
                queryInput.focus();
            });
        });

        queryInput && queryInput.focus();
    }

    // ====================== 事件处理 ======================

    function handleSubmit(e) {
        e.preventDefault();
        const query = queryInput.value.trim();
        if (!query || isLoading) return;
        sendQuery(query);
    }

    function handleKeydown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            const query = queryInput.value.trim();
            if (query && !isLoading) sendQuery(query);
        }
    }

    // ====================== 核心: 发送分析请求 ======================

    async function sendQuery(query) {
        isLoading = true;
        updateUI('loading');
        appendMessage('user', query);
        queryInput.value = '';

        try {
            const response = await fetch('/api/analyze', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query }),
            });

            if (!response.ok) {
                const err = await response.json().catch(() => ({}));
                throw new Error(err.detail || `HTTP ${response.status}`);
            }

            const data = await response.json();
            appendAnalysisResult(data);

        } catch (error) {
            appendMessage('error', `请求失败: ${error.message}`);
        } finally {
            isLoading = false;
            updateUI('idle');
        }
    }

    // ====================== 消息渲染 ======================

    function appendMessage(role, content) {
        const div = document.createElement('div');
        div.className = `message message-${role}`;

        if (role === 'user') {
            div.innerHTML = `
                <div class="message-avatar">👤</div>
                <div class="message-content">
                    <p>${escapeHtml(content)}</p>
                </div>`;
        } else if (role === 'error') {
            div.innerHTML = `
                <div class="message-avatar">⚠️</div>
                <div class="message-content message-error">
                    <p>${escapeHtml(content)}</p>
                </div>`;
        }

        chatMessages.appendChild(div);
        scrollToBottom();
    }

    function appendAnalysisResult(data) {
        const div = document.createElement('div');
        div.className = 'message message-agent';

        const statusIcon = data.success ? '✅' : '❌';
        const statusText = data.success ? '分析完成' : '分析失败';

        // 处理 Markdown 格式的摘要
        const summaryHtml = formatMarkdown(data.summary || '暂无分析结果');

        // 图表
        let chartsHtml = '';
        if (data.charts && data.charts.length > 0) {
            chartsHtml = `
                <div class="charts-container">
                    <h4>📊 生成的图表</h4>
                    ${data.charts.map(c => `
                        <div class="chart-item">
                            <img src="/api/charts/${encodeURIComponent(c.split('/').pop())}" 
                                 alt="图表" loading="lazy"
                                 onerror="this.parentElement.innerHTML='<p class=\\'chart-error\\'>图表加载失败</p>'" />
                        </div>
                    `).join('')}
                </div>`;
        }

        // 步骤详情（可折叠）
        let stepsHtml = '';
        if (data.steps && data.steps.length > 0) {
            stepsHtml = `
                <details class="steps-detail">
                    <summary>🔍 分析过程 (${data.step_count} 步)</summary>
                    <div class="steps-list">
                        ${data.steps.map((step, i) => `
                            <div class="step-item">
                                <span class="step-num">${i + 1}</span>
                                <span class="step-tool">${escapeHtml(step.tool || '')}</span>
                                <span class="step-desc">${escapeHtml(step.purpose || step.input || '')}</span>
                            </div>
                        `).join('')}
                    </div>
                </details>`;
        }

        // 元数据
        const elapsed = data.elapsed_ms ? `${(data.elapsed_ms / 1000).toFixed(1)}s` : '—';
        const metaHtml = `
            <div class="result-meta">
                <span>${statusIcon} ${statusText}</span>
                <span>⏱️ ${elapsed}</span>
                <span>🔎 ${data.sql_count || 0} 次查询</span>
            </div>`;

        div.innerHTML = `
            <div class="message-avatar">🤖</div>
            <div class="message-content">
                ${metaHtml}
                <div class="result-summary">${summaryHtml}</div>
                ${chartsHtml}
                ${stepsHtml}
            </div>`;

        chatMessages.appendChild(div);
        scrollToBottom();
    }

    // ====================== 工具函数 ======================

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function formatMarkdown(text) {
        // 简化 Markdown 渲染
        return text
            .replace(/```([\s\S]*?)```/g, '<pre><code>$1</code></pre>')
            .replace(/`([^`]+)`/g, '<code>$1</code>')
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.+?)\*/g, '<em>$1</em>')
            .replace(/^### (.+)$/gm, '<h4>$1</h4>')
            .replace(/^## (.+)$/gm, '<h3>$1</h3>')
            .replace(/^# (.+)$/gm, '<h2>$1</h2>')
            .replace(/^- (.+)$/gm, '<li>$1</li>')
            .replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>')
            .replace(/\n/g, '<br>');
    }

    function scrollToBottom() {
        if (chatMessages) {
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }
    }

    function updateUI(state) {
        if (state === 'loading') {
            if (sendBtn) {
                sendBtn.disabled = true;
                sendBtn.textContent = '分析中...';
            }
            if (queryInput) queryInput.disabled = true;

            // 添加加载动画
            const loader = document.createElement('div');
            loader.className = 'message message-loading';
            loader.id = 'loading-indicator';
            loader.innerHTML = `
                <div class="message-avatar">🤖</div>
                <div class="message-content">
                    <div class="typing-indicator">
                        <span></span><span></span><span></span>
                    </div>
                    <p class="loading-text">正在分析中，请稍候...</p>
                </div>`;
            chatMessages.appendChild(loader);
            scrollToBottom();

        } else {
            if (sendBtn) {
                sendBtn.disabled = false;
                sendBtn.textContent = '发送';
            }
            if (queryInput) {
                queryInput.disabled = false;
                queryInput.focus();
            }
            const loader = document.getElementById('loading-indicator');
            if (loader) loader.remove();
        }
    }

    // ====================== 启动 ======================
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();
