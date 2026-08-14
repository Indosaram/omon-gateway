mod pipeline;

pub use pipeline::{
    AudioDirection, AudioFrame, AudioFrameBuffer, AudioPayload, SpeechPipeline,
    SpeechPipelineOutput, SpeechToText, TextToSpeech, VoiceAudioPipeline, VoiceLanguageModel,
};

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use songbird::events::{Event, EventContext, EventHandler};
use tokio::sync::mpsc;

/// Songbird global event listener which forwards received Opus or decoded PCM
/// frames into the provider-independent voice pipeline.
pub struct SongbirdAudioEventListener {
    channel_id: u64,
    sender: mpsc::Sender<AudioFrame>,
    sequence: AtomicU64,
}

impl SongbirdAudioEventListener {
    pub fn new(channel_id: u64, sender: mpsc::Sender<AudioFrame>) -> Self {
        Self {
            channel_id,
            sender,
            sequence: AtomicU64::new(0),
        }
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }
}

#[async_trait]
impl EventHandler for SongbirdAudioEventListener {
    async fn act(&self, context: &EventContext<'_>) -> Option<Event> {
        match context {
            EventContext::VoiceTick(tick) => {
                for (source_id, voice) in &tick.speaking {
                    if let Some(samples) = &voice.decoded_voice {
                        let frame = AudioFrame::pcm(
                            self.channel_id,
                            Some(*source_id),
                            self.next_sequence(),
                            samples.clone(),
                        );
                        if self.sender.send(frame).await.is_err() {
                            break;
                        }
                    }
                }
            }
            EventContext::RtpPacket(packet) => {
                let end = packet.packet.len().saturating_sub(packet.payload_end_pad);
                if packet.payload_offset <= end {
                    let source_id = Some(packet.rtp().get_ssrc());
                    let frame = AudioFrame {
                        channel_id: self.channel_id,
                        source_id,
                        sequence: self.next_sequence(),
                        sample_rate: 48_000,
                        channels: 2,
                        direction: AudioDirection::Incoming,
                        payload: AudioPayload::Opus(
                            packet.packet[packet.payload_offset..end].to_vec(),
                        ),
                    };
                    let _ = self.sender.send(frame).await;
                }
            }
            _ => {}
        }
        None
    }
}
