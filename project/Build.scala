import sbt._

object ScaldingBuild extends Build {
  /**
   * An optional path to a local filesystem repository to add to the list of
   * resolvers.  This defaults to None, in which case it is not used.  To set a
   * path, place:
   *
   *   etsyFSRepoPath := "your/path/here"
   *
   * in build.sbt
   */
  val etsyFSRepoPath = SettingKey[Option[String]](
    "etsy-fs-repo-path",
    "Path to the local Etsy filesystem repository"
  )

  lazy val root = Project(
    "root",
    file("."),
    settings = Project.defaultSettings ++ Seq(etsyFSRepoPath := None)
  )

  /**
   * To keep code out of build.sbt, process the optional etsyFSRepoPath key
   * value here.
   */
  def optionallyAddEtsyFSRepo(path: Option[String]): Seq[sbt.Resolver] = {
    path.map(p =>
      Seq(Resolver.file(
        "filesystem-repo",
        file(p)
      )(
        Patterns(
          Seq(
            "[organisation]/[module]/[revision]/ivy-[revision].xml",
            "[organisation]/[module]/[revision]/ivys/ivy.xml"
          ),
          Seq(
            "[organisation]/[module]/[revision]/[type]s/[artifact]-[revision].[ext]",
            "[organisation]/[module]/[revision]/[type]s/[artifact].[ext]",
            "[organisation]/[module]/[revision]/[type]s/[artifact]-[classifier].[ext]"
          ),
          false
        )
      ))
    ).getOrElse(Seq())
  }
}
