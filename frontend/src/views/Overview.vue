<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "日志源", "created": 0, "pending": 0, "abnormal": 0}, {"name": "解析规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "脱敏规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件分类", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集作业", "created": 0, "pending": 0, "abnormal": 0}, {"name": "批次检查点", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件存储", "created": 0, "pending": 0, "abnormal": 0}, {"name": "异常告警", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采样归并", "created": 0, "pending": 0, "abnormal": 0}, {"name": "字段增强", "created": 0, "pending": 0, "abnormal": 0}, {"name": "归档策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "转发路由", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema版本", "created": 0, "pending": 0, "abnormal": 0}, {"name": "去重过滤", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集指标", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件重放", "created": 0, "pending": 0, "abnormal": 0}, {"name": "质量报告", "created": 0, "pending": 0, "abnormal": 0}, {"name": "标签分类", "created": 0, "pending": 0, "abnormal": 0}, {"name": "打包导出", "created": 0, "pending": 0, "abnormal": 0}, {"name": "访问审计", "created": 0, "pending": 0, "abnormal": 0}, {"name": "租户隔离", "created": 0, "pending": 0, "abnormal": 0}, {"name": "格式适配", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
