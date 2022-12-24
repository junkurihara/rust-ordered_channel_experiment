use anyhow::Result;
use std::collections::VecDeque;
use tokio::sync::mpsc;
// use std::sync::mpsc;
use tokio::sync::oneshot;

fn main() {
  let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
  runtime_builder.enable_all();
  runtime_builder.thread_name("channel_test");
  let runtime = runtime_builder.build().unwrap();

  runtime.block_on(async {
    entrypoint().await.unwrap();
  });
  println!("Exit the program");
}

const BUFFER_SIZE: usize = 32;
const CNT: u32 = 16;
type OneshotTx = oneshot::Sender<(u32, u32)>;
type OneshotRx = oneshot::Receiver<(u32, u32)>;
type NumOneshotTxTuple = (u32, OneshotTx);

async fn entrypoint() -> Result<()> {
  let (first_tx, first_rx) = mpsc::channel::<NumOneshotTxTuple>(BUFFER_SIZE);
  let (second_tx, second_rx) = mpsc::channel::<NumOneshotTxTuple>(BUFFER_SIZE);
  let (third_tx, third_rx) = mpsc::channel::<NumOneshotTxTuple>(BUFFER_SIZE);
  let (aggregator_tx, aggregator_rx) = mpsc::channel::<OneshotRx>(BUFFER_SIZE);

  // let (mid_tx, sink_rx) = mpsc::sync_channel(1);
  // let mid_tx_clone = mid_tx.clone();

  let task_first = tokio::spawn(async move { mid_task(1, first_rx).await });
  let task_second = tokio::spawn(async move { mid_task(2, second_rx).await });
  let task_third = tokio::spawn(async move { mid_task(3, third_rx).await });

  let generator_task = tokio::spawn(async move { generator_task(first_tx, second_tx, third_tx, aggregator_tx).await });
  let aggregator_task = tokio::spawn(async move { aggregator_task(aggregator_rx).await });

  let _ = task_first.await?;
  let _ = task_second.await?;
  let _ = task_third.await?;
  let _ = generator_task.await?;
  let _ = aggregator_task.await?;

  Ok(())
}

async fn generator_task(
  first_tx: mpsc::Sender<NumOneshotTxTuple>,
  second_tx: mpsc::Sender<NumOneshotTxTuple>,
  third_tx: mpsc::Sender<NumOneshotTxTuple>,
  aggregator_tx: mpsc::Sender<OneshotRx>,
) -> Result<()> {
  let mut oneshot_tx_queue = VecDeque::<OneshotTx>::new();
  // first send oneshot rxs to aggregator
  for i in 1..CNT + 1 {
    let (oneshot_tx, oneshot_rx) = oneshot::channel::<(u32, u32)>();
    oneshot_tx_queue.push_back(oneshot_tx);

    // spawn_blockingでsynchronous codeをspawnして、awaitで終了を待ち合わせる。
    // さらにblocking_sendを内部で呼び出すことで、synchronousに終了を待ち合わせる。
    // let aggregator_tx_clone = aggregator_tx.clone();
    // tokio::task::spawn_blocking(move || aggregator_tx_clone.blocking_send(oneshot_rx)).await??;

    aggregator_tx.send(oneshot_rx).await?; // これでも良くない？ ただ結局ブロックしてしまう。
  }

  for i in 1..CNT + 1 {
    let oneshot_tx = oneshot_tx_queue.pop_front().unwrap();

    if i % 3 == 2 {
      second_tx.send((i, oneshot_tx)).await?;
    } else if i % 3 == 1 {
      first_tx.send((i, oneshot_tx)).await?;
    } else {
      third_tx.send((i, oneshot_tx)).await?;
    }
  }
  Ok(())
}

async fn mid_task(id: u32, mut rx: mpsc::Receiver<NumOneshotTxTuple>) -> Result<()> {
  while let Some((num, oneshot_tx)) = rx.recv().await {
    if oneshot_tx.send((id, num)).is_err() {
      println!("failed to send : {}, {}", id, num);
    };
    // println!("{}", x);
  }
  Ok(())
}

async fn aggregator_task(mut rx: mpsc::Receiver<OneshotRx>) -> Result<()> {
  // ここでoneshot_rxは順序通りに飛んでくるはず。これを前から順に処理していく
  while let Some(oneshot_rx) = rx.recv().await {
    if let Ok((id, x)) = oneshot_rx.await {
      println!("id = {}: {}", id, x);
    } else {
      println!("omg");
    }
  }
  Ok(())
}
