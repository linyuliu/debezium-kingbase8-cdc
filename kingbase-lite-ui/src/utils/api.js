/**
 * API 请求工具函数
 */
export async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    throw new Error(`接口返回无法解析: ${response.status}`);
  }

  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `请求失败: ${response.status}`);
  }
  return payload.data;
}
