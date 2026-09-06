//! The public contract and model-specific conversion. No client repeats this math.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MODEL: &str = "pplx-embed-v1-0.6b";
pub const DIMENSIONS: usize = 1024;
/// Changing any output-affecting serving setting requires a new identifier and index rebuild.
pub const RECIPE: &str = "pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1";

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Input {
    Text(String),
    Batch(Vec<String>),
}

/// Accept only the documented text API, rejecting ignored or ambiguous options.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub model: String,
    pub input: Input,
    #[serde(default)]
    pub encoding_format: Option<String>,
}

impl Request {
    /// Bound batch cost before forwarding; vLLM performs authoritative token validation.
    pub fn validate(self) -> Result<Vec<String>, String> {
        if self.model != MODEL {
            return Err(format!("unsupported model; use {MODEL}"));
        }
        if self
            .encoding_format
            .as_deref()
            .is_some_and(|v| v != "float")
        {
            return Err("encoding_format must be float (JSON numbers)".into());
        }
        let inputs = match self.input {
            Input::Text(text) => vec![text],
            Input::Batch(batch) => batch,
        };
        if inputs.is_empty() || inputs.len() > 8 {
            return Err("input must contain 1..8 strings; split larger batches".into());
        }
        for (index, text) in inputs.iter().enumerate() {
            if text.trim().is_empty() || text.len() > 8192 {
                return Err(format!(
                    "input[{index}] must be nonblank and at most 8192 UTF-8 bytes; the backend also enforces 2048 tokens"
                ));
            }
        }
        if inputs.iter().map(String::len).sum::<usize>() > 32768 {
            return Err("batch exceeds 32768 UTF-8 bytes; split it".into());
        }
        Ok(inputs)
    }
}

#[derive(Debug, Deserialize)]
pub struct BackendResponse {
    pub data: Vec<BackendEmbedding>,
    pub usage: Value,
    pub model: String,
}

#[derive(Debug, Deserialize)]
pub struct BackendEmbedding {
    pub index: usize,
    pub embedding: Vec<f32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub object: String,
    pub model: String,
    pub data: Vec<Embedding>,
    pub usage: Value,
    pub embedding_recipe: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Embedding {
    pub object: String,
    pub index: usize,
    pub embedding: Vec<i8>,
}

/// Match Pplx's float32 tanh → ×127 → ties-to-even rounding → int8 recipe.
/// Rust's `round()` uses a different tie rule, so it must not be substituted here.
pub fn coordinate(value: f32) -> i8 {
    (value.tanh() * 127.0)
        .round_ties_even()
        .clamp(-128.0, 127.0) as i8
}

/// Reject malformed results rather than returning plausible but mismatched vectors.
pub fn transform(mut raw: BackendResponse, count: usize) -> Result<Response, String> {
    if raw.model != MODEL || raw.data.len() != count {
        return Err("backend returned an unexpected model or result count".into());
    }
    raw.data.sort_by_key(|entry| entry.index);
    let mut data = Vec::with_capacity(count);
    for (index, entry) in raw.data.into_iter().enumerate() {
        if entry.index != index
            || entry.embedding.len() != DIMENSIONS
            || !entry.embedding.iter().all(|v| v.is_finite())
        {
            return Err(
                "backend returned invalid indices, dimensions or nonfinite coordinates".into(),
            );
        }
        let embedding: Vec<_> = entry.embedding.into_iter().map(coordinate).collect();
        if embedding.iter().all(|v| *v == 0) {
            return Err("backend returned a zero embedding".into());
        }
        data.push(Embedding {
            object: "embedding".into(),
            index,
            embedding,
        });
    }
    Ok(Response {
        object: "list".into(),
        model: MODEL.into(),
        data,
        usage: raw.usage,
        embedding_recipe: RECIPE.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_coordinates() {
        let pairs: Vec<(f32, i8)> =
            serde_json::from_str(include_str!("../fixtures/transform.json")).unwrap();
        for (raw, expected) in pairs {
            assert_eq!(coordinate(raw), expected, "raw={raw}");
        }
    }

    #[test]
    fn refuses_bad_backend_shape_and_zero_vectors() {
        for embedding in [
            vec![1.; 8],
            vec![f32::NAN; DIMENSIONS],
            vec![0.; DIMENSIONS],
        ] {
            assert!(
                transform(
                    BackendResponse {
                        model: MODEL.into(),
                        usage: Value::Null,
                        data: vec![BackendEmbedding {
                            index: 0,
                            embedding
                        }]
                    },
                    1
                )
                .is_err()
            );
        }
    }

    #[test]
    fn restores_input_order_and_preserves_integer_coordinates() {
        let raw = BackendResponse {
            model: MODEL.into(),
            usage: Value::Null,
            data: vec![
                BackendEmbedding {
                    index: 1,
                    embedding: vec![-1.; DIMENSIONS],
                },
                BackendEmbedding {
                    index: 0,
                    embedding: vec![1.; DIMENSIONS],
                },
            ],
        };
        let response = transform(raw, 2).unwrap();
        assert_eq!(response.data[0].embedding[0], 97);
        assert_eq!(response.data[1].embedding[0], -97);
        assert_eq!(response.embedding_recipe, RECIPE);
    }
}
