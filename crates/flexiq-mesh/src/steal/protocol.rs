use flexiq_core::job::Job;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
pub struct StealRequest {
    pub thief_id: String,
    pub max_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StealResponse {
    pub jobs: Vec<Job>,
}

const MAX_FRAME_SIZE: usize = 1_048_576;

/// Write a length-prefixed bincode frame.
pub async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> std::io::Result<()> {
    let len = (data.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(data).await?;
    writer.flush().await
}

/// Read a length-prefixed bincode frame.
pub async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let req = StealRequest {
            thief_id: "w1".to_string(),
            max_count: 4,
        };
        let bytes = bincode::serialize(&req).unwrap();
        let decoded: StealRequest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.thief_id, "w1");
        assert_eq!(decoded.max_count, 4);
    }

    #[test]
    fn response_round_trip() {
        let resp = StealResponse { jobs: vec![] };
        let bytes = bincode::serialize(&resp).unwrap();
        let decoded: StealResponse = bincode::deserialize(&bytes).unwrap();
        assert!(decoded.jobs.is_empty());
    }

    #[tokio::test]
    async fn frame_round_trip() {
        let (client, server) = tokio::io::duplex(4096);
        let (_cr, mut cw) = tokio::io::split(client);
        let (mut sr, _sw) = tokio::io::split(server);

        let payload = b"hello mesh";
        write_frame(&mut cw, payload).await.unwrap();
        let received = read_frame(&mut sr).await.unwrap();
        assert_eq!(received, payload);
    }
}
