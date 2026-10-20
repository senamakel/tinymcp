use super::*;

#[test]
fn a_late_close_stdin_makes_the_legacy_last_call_predicate_red() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (gate, observer) = CloseStdinGate::new();
    let fake = FakeBox::serving_with_close_stdin_gate(Arc::clone(&gate));
    let spawner = JailedSpawner::new(fake.clone(), sandbox());
    let process = runtime.block_on(spawner.spawn_process(launch())).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    gate.set_deadline(deadline);
    let entered = observer.take_entered();
    let drop_gate = Arc::clone(&gate);
    let StdioProcess {
        stdin,
        stdout,
        handle,
    } = process;
    let dropper = std::thread::spawn(move || {
        drop(stdin);
        let entered = entered.recv_timeout(drop_gate.remaining());
        drop(stdout);
        drop(handle);
        entered
    });
    let entered = dropper.join().unwrap();
    assert!(
        entered.is_ok(),
        "the stdin pump did not enter the gated call: {:?}",
        fake.members()
    );
    assert!(
        observer
            .close_recorded
            .recv_timeout(gate.remaining())
            .is_ok(),
        "background release did not record Close: {:?}",
        fake.members()
    );

    observer.release_stdin_gate();
    assert!(
        observer
            .stdin_recorded
            .recv_timeout(gate.remaining())
            .is_ok(),
        "the delayed CloseStdin call was not recorded: {:?}",
        fake.members()
    );

    let members = fake.members();
    let kill = members
        .iter()
        .position(|member| *member == names::KILL_STDIO)
        .expect("the real drop cleanup recorded KillStdio");
    let close = members
        .iter()
        .position(|member| *member == names::CLOSE)
        .expect("the real drop cleanup recorded Close");
    let close_stdin = members
        .iter()
        .rposition(|member| *member == names::CLOSE_STDIN)
        .expect("the gated pump recorded CloseStdin after release");
    assert!(kill < close, "cleanup calls are out of order: {members:?}");
    assert!(
        close < close_stdin,
        "the gated stdin call was not recorded late: {members:?}"
    );
    assert_eq!(members.last(), Some(&names::CLOSE_STDIN));

    // Intentional controlled RED: retain the old timeout predicate for the
    // first runtime capture, after proving KillStdio and Close both occurred.
    assert!(
        members.last() == Some(&names::CLOSE),
        "legacy last-call predicate failed despite successful cleanup: {members:?}"
    );
}
