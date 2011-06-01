package de.batalski.twisscala.model;

/**
 * A timeline is a paginated List of Tweets, with a Long representing the timestamp of the next tweet on the page.
 *  If nextview is null, then we've reached the end of the Timeline.
 */
class Timeline(val view:List[Tweet],val nextview:Long) {    

}