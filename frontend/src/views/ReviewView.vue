<template>
  <section class="page" data-module="review_record">
    <header class="page-head">
      <div>
        <h2>历史复盘与调查采样</h2>
        <p class="page-desc">按可核验的入口凭证重算归档指纹；已派出样本不重发，未交付原行可继续领取。</p>
      </div>
      <div class="page-actions">
        <input
          v-model.trim="reviewId"
          class="review-input"
          data-testid="review-id"
          placeholder="复盘编号，如 RVW-ARCH-0001"
          @keyup.enter="load"
        />
        <button class="btn primary" data-testid="review-load" type="button" @click="load">继续复盘</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="review-error">{{ errorMessage }}</p>

    <template v-if="trace">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">会话状态</span>
          <strong class="stat-value" data-testid="session-status">{{ trace.session.status }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">待办原行</span>
          <strong class="stat-value" data-testid="pending-count">{{ trace.session.pending_record_ids.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已派出样本</span>
          <strong class="stat-value" data-testid="sample-count">{{ trace.samples.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">失配隔离</span>
          <strong class="stat-value" data-testid="mismatch-count">{{ trace.mismatches.length }}</strong>
        </article>
      </div>

      <div class="page-actions">
        <button class="btn primary" data-testid="review-resume" type="button" :disabled="busy" @click="resume">
          重启后续办
        </button>
        <button class="btn primary" data-testid="review-deliver" type="button" :disabled="busy" @click="deliver">
          领取剩余样本
        </button>
        <button class="btn" data-testid="review-ack" type="button" :disabled="busy || !awaitingAck.length" @click="acknowledgeAll">
          确认已派出样本（{{ awaitingAck.length }}）
        </button>
      </div>

      <h3>复盘项</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>记录编号</th>
            <th>归档行号</th>
            <th>状态</th>
            <th>指纹依据</th>
            <th>样本编号</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in trace.items"
            :key="item.record_id"
            :data-testid="`review-item-${item.record_id}`"
            :data-status="item.status"
          >
            <td>{{ item.record_id }}</td>
            <td>{{ item.row_index }}</td>
            <td>{{ item.status }}</td>
            <td>
              <span v-if="item.basis.fingerprint_fields">
                {{ (item.basis.fingerprint_fields as string[]).join('、') }}
                / {{ item.basis.value_source }}
                <span v-if="item.basis_locked" data-testid="basis-locked">（已锁定）</span>
              </span>
              <span v-else>—</span>
            </td>
            <td>{{ item.sample_id || '—' }}</td>
            <td>
              <span>{{ item.note }}</span>
              <ul v-if="item.mismatch" class="mismatch-list" data-testid="mismatch-locations">
                <li v-for="location in item.mismatch.locations" :key="location.field">
                  行 {{ item.mismatch.row_index }} · 第 {{ location.field_index ?? '—' }} 个指纹字段
                  <strong>{{ location.field }}</strong>：
                  归档 <code>{{ location.archived_value || '空' }}</code>
                  → 当前 <code>{{ location.current_value || '空' }}</code>
                  <span v-if="!location.in_fingerprint">（指纹字段之外）</span>
                </li>
              </ul>
            </td>
          </tr>
          <tr v-if="!trace.items.length">
            <td colspan="6" class="empty-state">该复盘暂无条目</td>
          </tr>
        </tbody>
      </table>

      <h3>调查样本</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>样本编号</th>
            <th>原行编号</th>
            <th>状态</th>
            <th>派发时间</th>
            <th>确认时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="sample in trace.samples" :key="sample.sample_id" :data-testid="`sample-${sample.sample_id}`">
            <td>{{ sample.sample_id }}</td>
            <td>{{ sample.record_id }}</td>
            <td>{{ sample.status }}</td>
            <td>{{ sample.dispatched_at || '—' }}</td>
            <td>{{ sample.ack_at || '—' }}</td>
          </tr>
          <tr v-if="!trace.samples.length">
            <td colspan="5" class="empty-state">还没有派出样本</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import {
  acknowledgeSample,
  deliverVerified,
  resumeReview,
  traceReview,
} from '@/api/review-service'
import type { ReviewTrace } from '@/api/review-service'

const reviewId = ref('')
const trace = ref<ReviewTrace | null>(null)
const errorMessage = ref('')
const busy = ref(false)

const awaitingAck = computed(
  () => trace.value?.samples.filter((sample) => sample.status === '已派出') ?? [],
)

async function run(task: () => Promise<void>) {
  busy.value = true
  errorMessage.value = ''
  try {
    await task()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复盘操作失败'
  } finally {
    busy.value = false
  }
}

function load() {
  if (!reviewId.value) {
    errorMessage.value = '请先填写复盘编号'
    return
  }
  void run(async () => {
    trace.value = await traceReview(reviewId.value)
  })
}

function resume() {
  if (!reviewId.value) {
    errorMessage.value = '请先填写复盘编号'
    return
  }
  void run(async () => {
    trace.value = await resumeReview(reviewId.value)
  })
}

function deliver() {
  if (!reviewId.value) return
  void run(async () => {
    trace.value = await deliverVerified(reviewId.value)
  })
}

function acknowledgeAll() {
  if (!reviewId.value || !trace.value) return
  void run(async () => {
    for (const sample of awaitingAck.value) {
      const result = await acknowledgeSample(reviewId.value, sample.sample_id)
      trace.value = result.review
    }
  })
}
</script>

<style scoped>
.review-input {
  min-width: 260px;
  padding: 0.45rem 0.6rem;
  border: 1px solid var(--border-color, #d0d7de);
  border-radius: 6px;
}

.mismatch-list {
  margin: 0.35rem 0 0;
  padding-left: 1.1rem;
  color: #b42318;
  font-size: 0.85rem;
}
</style>
