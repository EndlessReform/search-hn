//use std::io::Error;
//use std::{collections::HashMap, sync::Arc};

use crate::triton::{
    self, grpc_inference_service_client::GrpcInferenceServiceClient,
    model_infer_request::InferInputTensor,
};
use ndarray::Array2;
use tokenizers::tokenizer::Tokenizer;
//use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;
use tonic::{transport::Channel, Request, Status};

pub struct E5Embedder {
    client: GrpcInferenceServiceClient<Channel>,
    /// Caps concurrent requests from client
    /// Max length of document to be embedded
    max_seq_length: usize,
    tokenizer: Tokenizer,
}

#[derive(Debug)]
pub enum E5Error {
    ConnectError(String),
    TokenizerError(String),
    ParsingError(String),
}

impl From<tonic::transport::Error> for E5Error {
    fn from(err: tonic::transport::Error) -> Self {
        E5Error::ConnectError(err.to_string())
    }
}

impl From<Status> for E5Error {
    fn from(err: Status) -> Self {
        E5Error::ConnectError(err.to_string())
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for E5Error {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        E5Error::TokenizerError(err.to_string())
    }
}

fn parse_raw_output(
    data: Vec<Vec<u8>>,
    batch_size: usize,
    channel: usize,
    embed_depth: usize,
) -> Result<Vec<Array2<f32>>, E5Error> {
    if data.len() != batch_size {
        return Err(E5Error::ParsingError(
            "Output number of tensors does not match input!".into(),
        ));
    }

    // Check that all inputs are f32
    if data.iter().any(|v| v.len() % 4 != 0) {}

    let mut output = Vec::new();
    for chunk in data {
        // Convert the data bytes into f32s
        let len = chunk.len() / 4;
        if len % 4 != 0 {
            return Err(E5Error::ParsingError(
                "Raw data length is not a multiple of 4".into(),
            ));
        }

        let mut batch = Vec::with_capacity(len);
        for i in (0..chunk.len()).step_by(4) {
            let value = f32::from_le_bytes(
                chunk[i..i + 4]
                    .try_into()
                    .map_err(|_| E5Error::ParsingError("Raw data float size mismatch".into()))?,
            );
            batch.push(value);
        }
        let batch_array = Array2::from_shape_vec((channel, embed_depth), batch)
            .map_err(|_| E5Error::ParsingError("Embedding data could not match shape".into()))?;
        output.push(batch_array);
    }

    Ok(output)
}

impl E5Embedder {
    /// Initializes embedder for e5 small.
    /// TODO: Modularize this if need be!
    pub async fn new(url: &str) -> Result<Self, E5Error> {
        let client = GrpcInferenceServiceClient::connect(url.to_owned()).await?;
        // Blocking since the from_pretrained download method uses reqwest blocking.
        // If this leads to a perf decrease or bugs, abandon and download ourselves!
        let tokenizer = spawn_blocking(|| Tokenizer::from_pretrained("intfloat/e5-small-v2", None))
            .await
            .unwrap()?;
        //    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));
        Ok(Self {
            client,
            max_seq_length: 512,
            tokenizer,
        })
    }

    /// API will be changed, just a dummy to get comms w/ triton working
    pub async fn encode(&self, txt: &str) -> Result<Vec<Array2<f32>>, E5Error> {
        if txt.len() > self.max_seq_length {
            return Err(E5Error::TokenizerError(
                "Input sequence is too long!".into(),
            ));
        }
        let encoding = self.tokenizer.encode(txt, false)?;
        let mut client = self.client.clone();
        // Refer to e5-small-v2 Triton config.pbtxt
        let request = Request::new(triton::ModelInferRequest {
            model_name: "e5-small-v2".into(),
            model_version: "1".into(),
            inputs: vec![
                InferInputTensor {
                    name: "token_type_ids".into(),
                    datatype: "INT64".into(),
                    contents: Some(triton::InferTensorContents {
                        int64_contents: encoding.get_type_ids().iter().map(|x| *x as i64).collect(),
                        ..Default::default()
                    }),
                    shape: vec![encoding.n_sequences() as i64, encoding.len() as i64],
                    ..Default::default()
                },
                InferInputTensor {
                    name: "attention_mask".into(),
                    datatype: "INT64".into(),
                    contents: Some(triton::InferTensorContents {
                        int64_contents: encoding
                            .get_attention_mask()
                            .iter()
                            .map(|x| *x as i64)
                            .collect(),
                        ..Default::default()
                    }),
                    shape: vec![encoding.n_sequences() as i64, encoding.len() as i64],
                    ..Default::default()
                },
                InferInputTensor {
                    name: "input_ids".into(),
                    datatype: "INT64".into(),
                    contents: Some(triton::InferTensorContents {
                        int64_contents: encoding.get_ids().iter().map(|x| *x as i64).collect(),
                        ..Default::default()
                    }),
                    shape: vec![encoding.n_sequences() as i64, encoding.len() as i64],
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        let response = client.model_infer(request).await?.into_inner();
        //let outputs = response.outputs;
        let raw_outputs = response.raw_output_contents;
        println!("{:?}", raw_outputs.len());

        let embeddings =
            parse_raw_output(raw_outputs, encoding.n_sequences(), encoding.len(), 384)?;

        Ok(embeddings)
    }
}
