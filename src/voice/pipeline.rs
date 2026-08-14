use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

use crate::{OmonError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioDirection {
    Incoming,
    Outgoing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "codec", content = "samples", rename_all = "snake_case")]
pub enum AudioPayload {
    Opus(Vec<u8>),
    Pcm(Vec<i16>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AudioFrame {
    pub channel_id: u64,
    pub source_id: Option<u32>,
    pub sequence: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub direction: AudioDirection,
    pub payload: AudioPayload,
}

impl AudioFrame {
    pub fn pcm(channel_id: u64, source_id: Option<u32>, sequence: u64, samples: Vec<i16>) -> Self {
        Self {
            channel_id,
            source_id,
            sequence,
            sample_rate: 48_000,
            channels: 2,
            direction: AudioDirection::Incoming,
            payload: AudioPayload::Pcm(samples),
        }
    }

    pub fn serialized(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|error| OmonError::Config(error.to_string()))
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|error| OmonError::Config(error.to_string()))
    }

    pub fn byte_len(&self) -> usize {
        match &self.payload {
            AudioPayload::Opus(bytes) => bytes.len(),
            AudioPayload::Pcm(samples) => samples.len() * std::mem::size_of::<i16>(),
        }
    }
}

#[derive(Debug)]
pub struct AudioFrameBuffer {
    frames: VecDeque<AudioFrame>,
    capacity: usize,
}

impl AudioFrameBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            frames: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, frame: AudioFrame) -> Option<AudioFrame> {
        if self.capacity == 0 {
            return Some(frame);
        }
        let evicted = (self.frames.len() == self.capacity)
            .then(|| self.frames.pop_front())
            .flatten();
        self.frames.push_back(frame);
        evicted
    }

    pub fn pop(&mut self) -> Option<AudioFrame> {
        self.frames.pop_front()
    }

    pub fn drain(&mut self) -> Vec<AudioFrame> {
        self.frames.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn serialized(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.frames).map_err(|error| OmonError::Config(error.to_string()))
    }

    pub fn deserialize(bytes: &[u8], capacity: usize) -> Result<Self> {
        let frames: VecDeque<AudioFrame> =
            serde_json::from_slice(bytes).map_err(|error| OmonError::Config(error.to_string()))?;
        if frames.len() > capacity {
            return Err(OmonError::Config(format!(
                "audio buffer has {} frames but capacity is {capacity}",
                frames.len()
            )));
        }
        Ok(Self { frames, capacity })
    }
}

#[async_trait]
pub trait SpeechToText: Send + Sync + 'static {
    async fn transcribe(&self, frames: &[AudioFrame]) -> Result<String>;
}

#[async_trait]
pub trait VoiceLanguageModel: Send + Sync + 'static {
    async fn respond(&self, channel_id: u64, transcript: &str) -> Result<String>;
}

#[async_trait]
pub trait TextToSpeech: Send + Sync + 'static {
    async fn synthesize(&self, channel_id: u64, text: &str) -> Result<Vec<AudioFrame>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeechPipelineOutput {
    pub transcript: String,
    pub response: String,
    pub audio: Vec<AudioFrame>,
}

#[derive(Clone)]
pub struct SpeechPipeline {
    stt: Arc<dyn SpeechToText>,
    llm: Arc<dyn VoiceLanguageModel>,
    tts: Arc<dyn TextToSpeech>,
}

impl SpeechPipeline {
    pub fn new(
        stt: Arc<dyn SpeechToText>,
        llm: Arc<dyn VoiceLanguageModel>,
        tts: Arc<dyn TextToSpeech>,
    ) -> Self {
        Self { stt, llm, tts }
    }

    pub async fn process(
        &self,
        channel_id: u64,
        frames: &[AudioFrame],
    ) -> Result<SpeechPipelineOutput> {
        let transcript = self.stt.transcribe(frames).await?;
        let response = self.llm.respond(channel_id, &transcript).await?;
        let mut audio = self.tts.synthesize(channel_id, &response).await?;
        for frame in &mut audio {
            frame.direction = AudioDirection::Outgoing;
        }
        Ok(SpeechPipelineOutput {
            transcript,
            response,
            audio,
        })
    }
}

#[derive(Clone)]
pub struct VoiceAudioPipeline {
    sender: mpsc::Sender<AudioFrame>,
    receiver: Arc<Mutex<mpsc::Receiver<AudioFrame>>>,
}

impl VoiceAudioPipeline {
    pub fn channel(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }

    pub fn sender(&self) -> mpsc::Sender<AudioFrame> {
        self.sender.clone()
    }

    pub async fn submit(&self, frame: AudioFrame) -> Result<()> {
        self.sender
            .send(frame)
            .await
            .map_err(|_| OmonError::Config("voice audio pipeline is closed".into()))
    }

    pub async fn receive(&self) -> Option<AudioFrame> {
        self.receiver.lock().await.recv().await
    }
}
