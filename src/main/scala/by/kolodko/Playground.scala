package by.kolodko

import cats.effect.std.Queue
import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import fs2.{Chunk, INothing, Pipe, Pull, Pure, Stream}
import by.kolodko.Model.*
import by.kolodko.Data.*

import scala.util.Random
import scala.concurrent.duration._

object Playground extends IOApp {

  val jlActors: Stream[Pure, Actor] = Stream(
    henryCavil,
    galGodot,
    ezraMiller,
    benFisher,
    rayHardy,
    jasonMomoa
  )

  val tomHollandStream: Stream[Pure, Actor] = Stream.emit(tomHolland)
  val spiderMen: Stream[Pure, Actor] = Stream.emits(List(
    tomHolland,
    tobeyMaguire,
    andrewGarfield
  ))

  val jlActorList: List[Actor] = jlActors.toList
  val jlActorVector: Vector[Actor] = jlActors.toVector

  val infiniteJLActors: Stream[Pure, Actor] = jlActors.repeat
  val repeatedJLActorsList: List[Actor] = infiniteJLActors.take(12).toList

  val liftedJLActors: Stream[IO, Actor] = jlActors.covary[IO]

  import cats.MonadThrow
  def jlActorStream[F[_]: MonadThrow]: Stream[F, Actor] = jlActors.covary[F]

  val savingTomHolland: Stream[IO, Unit] = Stream.eval {
    IO {
      println(s"Saving actor $tomHolland")
      Thread.sleep(1000)
      println("Finished")
    }
  }

  val compiledStream: IO[Unit] = savingTomHolland.compile.drain

  val jlActorsEffectfulList: IO[List[Actor]] = liftedJLActors.compile.toList

  val avengersActors: Stream[Pure, Actor] = Stream.chunk(Chunk.array(Array(
    scarlettJohansson,
    robertDowneyJr,
    chrisEvans,
    markRuffalo,
    chrisHemsworth,
    jeremyRenner
  )))

  val dcAndMarvelSuperheroes: Stream[Pure, Actor] = jlActors ++ avengersActors

  val printedJLActors: Stream[IO, Unit] = jlActors.flatMap { actor =>
    Stream.eval(IO.println(actor))
  }

  val evalMappedJLActors: Stream[IO, Unit] = jlActors.evalMap(IO.println)

  // doesn't change the type of output
  val evalTappedJLActors: Stream[IO, Actor] = jlActors.evalTap(IO.println)

  val avengersActorsByFirstName: Stream[Pure, Map[String, List[Actor]]] = avengersActors.fold(Map.empty[String, List[Actor]]) { (map, actor) =>
    map + (actor.firstName -> (actor :: map.getOrElse(actor.firstName, Nil)))
  }

  val fromActorToStringPipe: Pipe[IO, Actor, String] = in =>
    in.map(actor => s"${actor.firstName} ${actor.lastName}")

  def toConsole[T]: Pipe[IO, T, Unit] = in =>
    in.evalMap(str => IO.println(str))

  val stringNamesOfJLActors: Stream[IO, Unit] =
    jlActors.through(fromActorToStringPipe).through(toConsole)

  object ActorRepository {
    def save(actor: Actor): IO[Int] = IO {
      println(s"Saving actor: $actor")
      if (Random.nextInt() % 2 == 0) {
        throw new RuntimeException("Something went wrong during the communication with the persistence layer")
      }
      println("Saved")
      actor.id
    }
  }

  val savedJLActors: Stream[IO, Int] = jlActors.evalMap(ActorRepository.save)

  val errorHandledSavedJLActors: Stream[IO, AnyVal] =
    savedJLActors.handleErrorWith(error => Stream.eval(IO.println(s"Error: $error")))

  val attemptedSavedJLActors: Stream[IO, Either[Throwable, Int]] = savedJLActors.attempt
  attemptedSavedJLActors.evalMap {
    case Left(error) => IO.println(s"Error: $error")
    case Right(id) => IO.println(s"Saved actor with id: $id")
  }

  case class DatabaseConnection(connection: String) extends AnyVal

  val acquire = IO {
    val conn = DatabaseConnection("jlaConnection")
    println(s"Acquiring connection to the database: $conn")
    conn
  }

  val release = (conn: DatabaseConnection) =>
    IO.println(s"Releasing connection to the database: $conn")

  val managedJLActors: Stream[IO, Int] =
    Stream.bracket(acquire)(release).flatMap(conn => savedJLActors)

  val tomHollandActorPull: Pull[Pure, Actor, Unit] = Pull.output1(tomHolland)
  val tomHollandActorStream: Stream[Pure, Actor] = tomHollandActorPull.stream

  val spiderMenActorPull: Pull[Pure, Actor, Unit] =
    tomHollandActorPull >> Pull.output1(tobeyMaguire) >> Pull.output1(andrewGarfield)

  val avengersActorsPull: Pull[Pure, Actor, Unit] = avengersActors.pull.echo

  val unconsAvengersActors: Pull[Pure, INothing, Option[(Chunk[Actor], Stream[Pure, Actor])]] =
    avengersActors.pull.uncons

  val uncons1AvengersActor: Pull[Pure, INothing, Option[(Actor, Stream[Pure, Actor])]] =
    avengersActors.pull.uncons1

  def takeByName(name: String): Pipe[IO, Actor, Actor] = {
    def go(s: Stream[IO, Actor], name: String): Pull[IO, Actor, Unit] =
      s.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((hd, tl)) =>
          if (hd.firstName == name) Pull.output1(hd) >> go(tl, name)
          else go(tl, name)
      }

    in => go(in, name).stream
  }

  val avengersActorsCalledChris: Stream[IO, Unit] =
    avengersActors.through(takeByName("Chris")).through(toConsole)

  val concurrentJLActors: Stream[IO, Actor] = liftedJLActors.evalMap(actor => IO {
    Thread.sleep(400)
    actor
  })
  val liftedAvengersActors: Stream[IO, Actor] = avengersActors.covary[IO]
  val concurrentAvengersActors: Stream[IO, Actor] = liftedAvengersActors.evalMap(actor => IO {
    Thread.sleep(200)
    actor
  })
  val mergedHeroesActors: Stream[IO, Unit] =
    concurrentJLActors.merge(concurrentAvengersActors).through(toConsole)

  val queue: IO[Queue[IO, Actor]] = Queue.bounded[IO, Actor](10)
  val concurrentlyStreams: Stream[IO, Unit] = Stream.eval(queue).flatMap { q =>
    val producer: Stream[IO, Unit] =
      liftedJLActors
        .evalTap(actor => IO.println(s"[${Thread.currentThread().getName}] produced $actor"))
        .evalMap(q.offer)
        .metered(1.second)
    val consumer: Stream[IO, Unit] =
      Stream.fromQueueUnterminated(q)
        .evalMap(actor => IO.println(s"[${Thread.currentThread().getName}] consumed $actor"))
    producer.concurrently(consumer)
  }

  val toConsoleWithThread: Pipe[IO, Actor, Unit] = in =>
    in.evalMap(actor => IO.println(s"[${Thread.currentThread().getName}] consumed $actor"))

  val parJoinedActors: Stream[IO, Unit] =
    Stream(
      jlActors.through(toConsoleWithThread),
      avengersActors.through(toConsoleWithThread),
      spiderMen.through(toConsoleWithThread)
    ).parJoin(4)

  override def run(args: List[String]): IO[ExitCode] = {
    parJoinedActors.compile.drain.as(ExitCode.Success)
  }

}
