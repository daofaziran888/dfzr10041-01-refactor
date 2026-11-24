/** 历史复盘接口封装：开审、补送凭证、交付、确认、续办与回查。 */
import { request } from './client'

export type ReviewItem = {
  review_id: string
  record_id: string
  row_index: number
  status: string
  recorded_fingerprint: string
  recomputed_fingerprint: string
  basis: Record<string, unknown>
  mismatch: {
    isolated: boolean
    row_index: number
    original_location: string
    locations: {
      field: string
      field_index: number | null
      archived_value: string
      current_value: string
      in_fingerprint: boolean
    }[]
  } | null
  sample_id: string
  dispatched_at: string
  ack_at: string
  basis_locked: boolean
  credential_revision: number
  note: string
}

export type InvestigationSample = {
  sample_id: string
  review_id: string
  record_id: string
  status: string
  basis_locked: boolean
  dispatched_at: string
  ack_at: string
}

export type ReviewTrace = {
  session: {
    review_id: string
    batch_id: string
    credential_id: string
    credential_revision: number
    status: string
    pending_record_ids: string[]
  }
  items: ReviewItem[]
  samples: InvestigationSample[]
  mismatches: {
    record_id: string
    row_index: number
    original_location: string
    locations: { field: string; field_index: number | null; archived_value: string; current_value: string }[]
  }[]
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: body === undefined ? '{}' : JSON.stringify(body),
  })
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(String(payload?.detail ?? `接口返回 ${response.status}`))
  }
  return payload as T
}

export async function traceReview(reviewId: string): Promise<ReviewTrace> {
  const response = await request(`/api/reviews/${encodeURIComponent(reviewId)}`)
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(String(payload?.detail ?? `接口返回 ${response.status}`))
  }
  return payload as ReviewTrace
}

export function resumeReview(reviewId: string): Promise<ReviewTrace> {
  return post<ReviewTrace>(`/api/reviews/${encodeURIComponent(reviewId)}/resume`)
}

export function deliverVerified(reviewId: string): Promise<ReviewTrace> {
  return post<ReviewTrace>(`/api/reviews/${encodeURIComponent(reviewId)}/deliver`)
}

export function acknowledgeSample(
  reviewId: string,
  sampleId: string,
): Promise<{ first_ack: boolean; review: ReviewTrace }> {
  return post(`/api/reviews/${encodeURIComponent(reviewId)}/acknowledge`, {
    sample_id: sampleId,
  })
}

export function submitCredential(
  reviewId: string,
  credential: Record<string, unknown>,
  correction = false,
): Promise<ReviewTrace> {
  return post<ReviewTrace>(`/api/reviews/${encodeURIComponent(reviewId)}/credentials`, {
    credential,
    correction,
  })
}
