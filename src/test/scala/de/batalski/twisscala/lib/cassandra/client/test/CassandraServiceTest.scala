/**
 *
 */
package de.batalski.twisscala.lib.cassandra.client.test
import de.batalski.twisscala.lib.cassandra.client._
import org.specs.Specification
import de.batalski.twisscala.model.TwisscalaUtils
import de.batalski.twisscala.model.User

/**
 * @author gbatalski
 *
 */
class CassandraServiceTest extends Specification {

  
  test
  def test = {
    
    TwisscalaUtils.createModel
    
    val user = new User("huj","pizda")
    
    TwisscalaUtils saveUser user
    
    TwisscalaUtils.getUserByUsername("huj") mustNotBe empty 
  }
}