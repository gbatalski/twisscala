/**
 *
 */
package de.batalski.lib.cassandra.client.test
import de.batalski.lib.cassandra.client._
import org.specs.Specification

/**
 * @author gbatalski
 *
 */
class CassandraServiceTest extends Specification {

  val cassService = CassandraService.apply
  val CF = "TestCF"
  test
  def test = {
    cassService.createColumnFamilyIfAbsent(CF)
    cassService.updateColumn("huj",
      "pizda",
      "password",
      CF)
    cassService.updateColumn("huj",
      "pizda",
      "password2",
      CF)
    cassService.updateColumn("huj",
      "pizda",
      "password3",
      CF)
     
     cassService.listColumns("huj",CF) mustNotBe empty
  }
}