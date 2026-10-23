// UI strings. A value is a string with {name} placeholders, or a function of
// the params for wording that depends on them (e.g. English plurals).

const plural = (n, one, many) => (n === 1 ? one : many);

const zh = {
  'lang.switch': 'EN',
  'lang.switchTitle': 'Switch to English',

  'common.cancel': '取消',
  'common.confirm': '确认',
  'common.running': '执行中…',
  'common.save': '保存',
  'common.close': '关闭',
  'common.error': '错误',
  'common.queryFailed': '查询失败',

  'header.filter': '筛选 job / queue / server / URL',
  'header.interval': '自动刷新间隔',
  'header.resume': '▶ 继续',
  'header.pause': '⏸ 暂停',
  'header.refresh': '刷新',
  'header.reload': 'Reload 配置',
  'header.token': 'Token',

  'app.authFailed': '鉴权失败：请设置正确的 token（或导入客户端证书）。',
  'app.loadFailed': '加载失败：',
  'app.loading': '加载中…',
  'app.updatedAt': '更新于 {time}',
  'app.refreshPaused': ' · 自动刷新已暂停',
  'app.rateNote': ' · 速率按相邻两次刷新计算',

  'summary.jobs': 'Jobs',
  'summary.workers': 'Workers',
  'summary.backlog': '积压消息',
  'summary.dlq': 'DLQ 消息',
  'summary.failed': 'Failed',
  'summary.depthErrors': '队列查询失败',

  'table.empty': '没有匹配的 job',
  'table.backlog': '积压',
  'badge.paused': '已暂停',
  'badge.block': '拦截',
  'badge.blockDiscard': '拦截并丢弃',
  'badge.auto': '自动 {min}–{max}',
  'badge.autoTitle': 'worker 数按队列积压自动调整',

  'confirm.typeBefore': '请输入',
  'confirm.typeAfter': '确认',

  'token.label': 'Bearer token（仅保存在当前标签页的 sessionStorage）',
  'token.hint': '使用 mTLS 时，浏览器需导入客户端证书；未配置 token 时留空即可。',
  'token.clear': '清除',

  'reload.title': '重新加载配置',
  'reload.previewing': '正在预览（dry run）…',
  'reload.diff': '以下是配置文件与当前运行状态的差异：',
  'reload.added': '新增',
  'reload.removed': '删除',
  'reload.updated': '更新',
  'reload.scaled': '扩缩容',
  'reload.recreated': '重建',
  'reload.settings': '其他设置',
  'reload.restartRequired': '需重启生效',
  'reload.unchanged': '未变化',
  'reload.unchangedCount': '{n} 个 job',
  'reload.nothing': '没有需要应用的变更。',
  'reload.recreatedWarn': '重建的 job 会丢弃通过 Admin API 做的临时修改。',
  'reload.apply': '应用',
  'reload.applying': '应用中…',
  'reload.done': '配置已重新加载',

  'tab.settings': '设置',
  'tab.messages': '消息',
  'tab.try': '试投',

  'settings.set': '设置',
  'settings.pause': '暂停（设为 0）',
  'settings.workersHint': '范围 0–{max}。设为 0 时每个 worker 会先处理完手上的消息。',
  'settings.workersDone': '把 {id} 的 worker 数设为 {n}',
  'settings.workersDoneAuto': '把 {id} 的 worker 数设为 {n}，autoscale 已暂停',
  'settings.workersHintAuto': '当前开启了 autoscale，手动设置会暂停 autoscale，需要在下方重新开启。',
  'autoscale.enabled': '按队列积压自动调整 worker 数',
  'autoscale.min': '最小',
  'autoscale.max': '最大',
  'autoscale.messages_per_worker': '每 worker 消息数',
  'autoscale.interval_seconds': '检查间隔（秒）',
  'autoscale.scale_down_delay_seconds': '缩容延迟（秒）',
  'autoscale.max_step_up': '单次最多扩容',
  'autoscale.max_failure_ratio': '失败率上限',
  'autoscale.help.min': 'worker 数下限，可以为 0',
  'autoscale.help.max': 'worker 数上限，开启时必填',
  'autoscale.help.messages_per_worker': '目标 worker 数 = ceil(积压 / 该值)',
  'autoscale.help.interval_seconds': '多久查询一次队列积压',
  'autoscale.help.scale_down_delay_seconds': '积压持续低于当前处理能力这么久之后才缩容；扩容立即生效',
  'autoscale.help.max_step_up': '每次检查最多增加的 worker 数，0 表示不限',
  'autoscale.help.max_failure_ratio': '上一个检查周期内失败消息占比超过该值（且至少 20 条）时不再扩容，1 表示不检查',
  'autoscale.done': '已更新 {id} 的 autoscale 设置',
  'autoscale.hint': '只按主队列的 ready 消息数计算（已投递给 worker、未 ack 的消息不计入）。每个副本独立计算，最小/最大值按单个副本算。',
  'settings.urlDone': '已更新 {id} 的 URL',
  'settings.patternsPlaceholder': '正则表达式，留空表示不拦截',
  'settings.patternsDone': '已更新 {id} 的 block patterns',
  'settings.discard': '命中时直接丢弃（discard_match_jobs）',
  'settings.discardDone': '已更新 {id} 的 discard 设置',
  'settings.memoryNote': '通过界面做的修改只保存在内存中，重启或该项在配置文件中变更并 reload 后会被覆盖。',

  'msg.mainQueue': '主队列',
  'msg.inspect': '查看与操作',
  'msg.count': '条数',
  'msg.peekMain': '窥视主队列',
  'msg.peekDlq': '窥视 DLQ',
  'msg.peekMainLabel': '主队列窥视',
  'msg.peekDlqLabel': 'DLQ 窥视',
  'msg.take': '取出（take）',
  'msg.takeTitle': '取出消息',
  'msg.takeConfirm': '从 {queue} 取出最多 {n} 条消息并 ack，这些消息将从队列中删除，不会被投递。',
  'msg.takenLabel': '已取出（已从队列删除）',
  'msg.replay': '重放 DLQ',
  'msg.replayConfirm': '把 {dlq} 中最多 {n} 条消息重新投递到 {queue}。',
  'msg.replayDone': '已重放 {n} 条消息',
  'msg.peekHint': '窥视不会 ack，消息会退回队列，但其 redelivered 标记会被置位；单次最多 {max} 条。',
  'msg.purge': '清空',
  'msg.purgeMain': '清空主队列',
  'msg.purgeMainConfirm': '将删除 {queue} 中所有待投递（ready）的消息，不可恢复。建议先把 worker 设为 0。',
  'msg.stillConsuming': '当前仍有 {n} 个 worker 在消费。',
  'msg.purgeMainDone': '已清空 {n} 条消息',
  'msg.purgeDlq': '清空 DLQ',
  'msg.purgeDlqConfirm': '将删除 {dlq} 中所有消息，不可恢复。',
  'msg.purgeDlqDone': '已清空 DLQ {n} 条消息',
  'msg.purgeHint': '多副本部署时，请先暂停所有实例的 worker 再清空，见 README「清空队列」。',
  'msg.resultCount': '{label}：{n} 条',
  'msg.empty': '队列中没有可读取的消息',

  'try.title': '试投',
  'try.hintBefore': '把消息体直接 POST 到 consumer（',
  'try.hintAfter': '），不经过队列。',
  'try.placeholder': '消息体',
  'try.send': '发送',
  'try.failed': '失败',
  'try.ok': '成功',
};

const en = {
  'lang.switch': '中文',
  'lang.switchTitle': '切换到中文',

  'common.cancel': 'Cancel',
  'common.confirm': 'Confirm',
  'common.running': 'Running…',
  'common.save': 'Save',
  'common.close': 'Close',
  'common.error': 'Error',
  'common.queryFailed': 'Query failed',

  'header.filter': 'Filter by job / queue / server / URL',
  'header.interval': 'Auto-refresh interval',
  'header.resume': '▶ Resume',
  'header.pause': '⏸ Pause',
  'header.refresh': 'Refresh',
  'header.reload': 'Reload config',
  'header.token': 'Token',

  'app.authFailed': 'Authentication failed: set a valid token (or import a client certificate).',
  'app.loadFailed': 'Failed to load: ',
  'app.loading': 'Loading…',
  'app.updatedAt': 'Updated at {time}',
  'app.refreshPaused': ' · auto-refresh paused',
  'app.rateNote': ' · rates are computed between two consecutive refreshes',

  'summary.jobs': 'Jobs',
  'summary.workers': 'Workers',
  'summary.backlog': 'Backlog',
  'summary.dlq': 'DLQ messages',
  'summary.failed': 'Failed',
  'summary.depthErrors': 'Queue query errors',

  'table.empty': 'No matching jobs',
  'table.backlog': 'Backlog',
  'badge.paused': 'Paused',
  'badge.block': 'Blocking',
  'badge.blockDiscard': 'Blocking + discard',
  'badge.auto': 'Auto {min}–{max}',
  'badge.autoTitle': 'Workers follow the queue backlog',

  'confirm.typeBefore': 'Type',
  'confirm.typeAfter': 'to confirm',

  'token.label': 'Bearer token (kept in this tab\'s sessionStorage only)',
  'token.hint': 'With mTLS, import the client certificate into the browser. Leave empty if no token is configured.',
  'token.clear': 'Clear',

  'reload.title': 'Reload config',
  'reload.previewing': 'Previewing (dry run)…',
  'reload.diff': 'Differences between the config file and the running state:',
  'reload.added': 'Added',
  'reload.removed': 'Removed',
  'reload.updated': 'Updated',
  'reload.scaled': 'Scaled',
  'reload.recreated': 'Recreated',
  'reload.settings': 'Other settings',
  'reload.restartRequired': 'Restart required',
  'reload.unchanged': 'Unchanged',
  'reload.unchangedCount': ({ n }) => `${n} ${plural(n, 'job', 'jobs')}`,
  'reload.nothing': 'No changes to apply.',
  'reload.recreatedWarn': 'Recreated jobs drop temporary changes made through the Admin API.',
  'reload.apply': 'Apply',
  'reload.applying': 'Applying…',
  'reload.done': 'Config reloaded',

  'tab.settings': 'Settings',
  'tab.messages': 'Messages',
  'tab.try': 'Try',

  'settings.set': 'Set',
  'settings.pause': 'Pause (set to 0)',
  'settings.workersHint': 'Range 0–{max}. At 0, each worker finishes its current message first.',
  'settings.workersDone': 'Set workers of {id} to {n}',
  'settings.workersDoneAuto': 'Set workers of {id} to {n}; autoscale paused',
  'settings.workersHintAuto': 'Autoscale is on: setting workers by hand pauses it until you turn it on again below.',
  'autoscale.enabled': 'Scale workers with the queue backlog',
  'autoscale.min': 'Min',
  'autoscale.max': 'Max',
  'autoscale.messages_per_worker': 'Messages per worker',
  'autoscale.interval_seconds': 'Interval (s)',
  'autoscale.scale_down_delay_seconds': 'Scale-down delay (s)',
  'autoscale.max_step_up': 'Max step up',
  'autoscale.max_failure_ratio': 'Max failure ratio',
  'autoscale.help.min': 'Lower bound; may be 0',
  'autoscale.help.max': 'Upper bound; required when enabled',
  'autoscale.help.messages_per_worker': 'Desired workers = ceil(backlog / this)',
  'autoscale.help.interval_seconds': 'How often the queue backlog is checked',
  'autoscale.help.scale_down_delay_seconds': 'Workers are removed only after the backlog stays low this long; scaling up is immediate',
  'autoscale.help.max_step_up': 'Most workers added per check; 0 means no limit',
  'autoscale.help.max_failure_ratio': 'No scaling up while more than this share of messages failed in the last interval (at least 20 finished); 1 disables the check',
  'autoscale.done': 'Updated autoscale of {id}',
  'autoscale.hint': 'Based only on ready messages in the main queue (messages delivered to workers but not yet acked are not counted). Each replica scales on its own; min and max are per replica.',
  'settings.urlDone': 'Updated URL of {id}',
  'settings.patternsPlaceholder': 'Regular expression; empty disables blocking',
  'settings.patternsDone': 'Updated block patterns of {id}',
  'settings.discard': 'Discard matching messages (discard_match_jobs)',
  'settings.discardDone': 'Updated discard setting of {id}',
  'settings.memoryNote': 'Changes made here are kept in memory only. They are lost on restart, or overwritten when the same setting changes in the config file and is reloaded.',

  'msg.mainQueue': 'Main queue',
  'msg.inspect': 'Inspect & act',
  'msg.count': 'Count',
  'msg.peekMain': 'Peek queue',
  'msg.peekDlq': 'Peek DLQ',
  'msg.peekMainLabel': 'Queue peek',
  'msg.peekDlqLabel': 'DLQ peek',
  'msg.take': 'Take',
  'msg.takeTitle': 'Take messages',
  'msg.takeConfirm': ({ queue, n }) => `Take and ack up to ${n} ${plural(n, 'message', 'messages')} from ${queue}. They are removed from the queue and will not be delivered.`,
  'msg.takenLabel': 'Taken (removed from the queue)',
  'msg.replay': 'Replay DLQ',
  'msg.replayConfirm': ({ dlq, queue, n }) => `Republish up to ${n} ${plural(n, 'message', 'messages')} from ${dlq} to ${queue}.`,
  'msg.replayDone': ({ n }) => `Replayed ${n} ${plural(n, 'message', 'messages')}`,
  'msg.peekHint': 'Peek does not ack: messages return to the queue, but are marked redelivered. At most {max} per call.',
  'msg.purge': 'Purge',
  'msg.purgeMain': 'Purge queue',
  'msg.purgeMainConfirm': 'All ready messages in {queue} will be deleted permanently. Consider setting workers to 0 first.',
  'msg.stillConsuming': ({ n }) => ` ${n} ${plural(n, 'worker is', 'workers are')} still consuming.`,
  'msg.purgeMainDone': ({ n }) => `Purged ${n} ${plural(n, 'message', 'messages')}`,
  'msg.purgeDlq': 'Purge DLQ',
  'msg.purgeDlqConfirm': 'All messages in {dlq} will be deleted permanently.',
  'msg.purgeDlqDone': ({ n }) => `Purged ${n} DLQ ${plural(n, 'message', 'messages')}`,
  'msg.purgeHint': 'With multiple replicas, pause the workers of every instance before purging. See "清空队列" (purge) in the README.',
  'msg.resultCount': ({ label, n }) => `${label}: ${n} ${plural(n, 'message', 'messages')}`,
  'msg.empty': 'No messages available in the queue',

  'try.title': 'Try',
  'try.hintBefore': 'POSTs the body directly to the consumer (',
  'try.hintAfter': '), bypassing the queue.',
  'try.placeholder': 'Message body',
  'try.send': 'Send',
  'try.failed': 'Failed',
  'try.ok': 'Succeeded',
};

const dicts = { zh, en };
const LANG_KEY = 'cd.lang';

// BCP 47 tags for <html lang> and number/time formatting.
export const locales = { zh: 'zh-CN', en: 'en' };

function initialLang() {
  const saved = localStorage.getItem(LANG_KEY);
  if (saved in dicts) return saved;
  return (navigator.language || '').toLowerCase().startsWith('zh') ? 'zh' : 'en';
}

let lang = initialLang();

export const getLang = () => lang;

export function setLang(next) {
  lang = next;
  localStorage.setItem(LANG_KEY, next);
  document.documentElement.lang = locales[next];
}

document.documentElement.lang = locales[lang];

export function t(key, params = {}) {
  const v = dicts[lang][key] ?? zh[key] ?? key;
  if (typeof v === 'function') return v(params);
  return v.replace(/\{(\w+)\}/g, (m, name) => (name in params ? String(params[name]) : m));
}
