package io.andygrove.spark.server

import com.datasift.dropwizard.scala.ScalaApplication
import io.dropwizard.Configuration
import io.dropwizard.setup.{Bootstrap, Environment}

class MyConfiguration extends Configuration {
}

object QueryServer extends ScalaApplication[MyConfiguration] {

  override def init(bootstrap: Bootstrap[MyConfiguration]) {
  }

  def run(conf: MyConfiguration, env: Environment) {
    env.jersey().register(new SharedContextQueryResource(conf))
    env.jersey().register(new ContextPerQueryResource(conf))
  }

}





