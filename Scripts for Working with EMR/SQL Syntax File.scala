import scala.io.Source
 
val filePath = "sql_syntax.sql"

val fileContents: String = Source.fromFile(filePath).getLines().mkString("\n")
 
// Print or use the contents
println(fileContents)