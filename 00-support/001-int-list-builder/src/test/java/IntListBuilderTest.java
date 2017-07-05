import java.util.List;

import org.joolzminer.examples.spark.java.IntListBuilder;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * 
 * Test class for IntListBuilder
 *
 *	start: starting value
 *	end  : end value
 *	step : optional step
 */
public class IntListBuilderTest {

	/*
	 * 	start = end
	 * 	no step
	 */
	@Test
	public void testStartEqualEndNoStepReturnsListWithOneElem() {
		List<Integer> nums = new IntListBuilder()
									.from(0)
									.to(0)
									.build();
		
		assertThat(nums, hasSize(1));
		assertThat(nums, contains(0));		
	}
	
	/*
	 * start = end -1
	 * no step
	 */
	@Test
	public void testStartIsEndMinusOneNoStepReturnsListWithTwoElems() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(1)
				.build();

		assertThat(nums, hasSize(2));
		assertThat(nums, contains(0, 1));
	}
	
	/*
	 * start = end - 2
	 * no step
	 */
	@Test
	public void testStartIsEndMinusTwoNoStepReturnsListWithThreeElems() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(2)
				.build();

		assertThat(nums, hasSize(3));
		assertThat(nums, contains(0, 1, 2));
	}	
	
	/*
	 * start = end + 1
	 * no step
	 */
	@Test
	public void testStartIsEndPlusOneNoStepReturnsListWithTwoElemsInDescOrder() {
		List<Integer> nums = new IntListBuilder()
				.from(1)
				.to(0)
				.build();

		assertThat(nums, hasSize(2));
		assertThat(nums, contains(1, 0));
	}
	
	/*
	 * start = end + 2
	 * no step
	 */
	@Test
	public void testStartIsEndPlusTwoNoStepReturnsListWithThreeElemsInDescOrder() {
		List<Integer> nums = new IntListBuilder()
				.from(2)
				.to(0)
				.build();

		assertThat(nums, hasSize(3));
		assertThat(nums, contains(2, 1, 0));	
	}	
	
	/*
	 * start = end
	 * step = 1
	 */
	@Test
	public void testStartEqualEndWithStepOneReturnsListWithOneElem() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(0)
				.by(1)
				.build();

		assertThat(nums, hasSize(1));
		assertThat(nums, contains(0));
	}
	
	/*
	 * start = end
	 * step = 2
	 */
	@Test
	public void testStartEqualEndWithStepTwoReturnsListWithOneElem() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(0)
				.by(2)
				.build();

		assertThat(nums, hasSize(1));
		assertThat(nums, contains(0));		
	}
	
	/*
	 * start = end - 2
	 * step = 2
	 */
	@Test
	public void testStartEqualEndMinusTwoWithStepTwoReturnsListWithTwoElems() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(2)
				.by(2)
				.build();

		assertThat(nums, hasSize(2));
		assertThat(nums, contains(0, 2));		
	}
	
	/*
	 * start = end - 3
	 * step = 2
	 */
	@Test
	public void testStartEqualEndMinusThreeWithStepTwoReturnsListWithOneElem() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(3)
				.by(2)
				.build();

		assertThat(nums, hasSize(2));
		assertThat(nums, contains(0, 2));
	}	
	
	
	/*
	 * start = end + 1
	 * step = 1
	 */
	@Test
	public void testStartEqualEndPlusOneWithStepTwoReturnsListWithOTwoElems() {
		List<Integer> nums = new IntListBuilder()
				.from(1)
				.to(0)
				.by(1)
				.build();

		assertThat(nums, hasSize(2));
		assertThat(nums, contains(1, 0));
	}		
	
	/*
	 * start = end + 2
	 * step = 1
	 */
	@Test
	public void testStartEqualEndPlusTwoWithStepTwoReturnsListWithTwoElemsInDesc() {
		List<Integer> nums = new IntListBuilder()
				.from(2)
				.to(0)
				.by(1)
				.build();

		assertThat(nums, hasSize(3));
		assertThat(nums, contains(2, 1, 0));
	}		
	
	/*
	 * start = end + 2
	 * step = 2
	 */
	@Test
	public void testStartEqualEndPlusTwoWithStepTwoReturnsListWithTwoElems() {
		List<Integer> nums = new IntListBuilder()
				.from(2)
				.to(0)
				.by(2)
				.build();

		assertThat(nums, hasSize(2));
		assertThat(nums, contains(2, 0));
	}		
	
	/*
	 * start = end + 4
	 * step = 2
	 */
	@Test
	public void testStartEqualEndPlusTwoWithStepTwoReturnsListWithFourElems() {
		List<Integer> nums = new IntListBuilder()
				.from(4)
				.to(0)
				.by(2)
				.build();

		assertThat(nums, hasSize(3));
		assertThat(nums, contains(4, 2, 0));
	}		
	
	/*
	 *  new IntListBuilder().from(0).to(10).by(2) => { 0, 2, 4, 6, 8, 10 }
	 */
	@Test
	public void testUsageExample3() {
		List<Integer> nums = new IntListBuilder()
				.from(0)
				.to(10)
				.by(2)
				.build();

		assertThat(nums, hasSize(6));
		assertThat(nums, contains(0, 2, 4, 6, 8, 10));
	}		

	/*
	 *  new IntListBuilder().from(5).to(3) => { 5, 4, 3 }
	 */
	@Test
	public void testUsageExample4() {
		List<Integer> nums = new IntListBuilder()
				.from(5)
				.to(3)
				.build();

		assertThat(nums, hasSize(3));
		assertThat(nums, contains(5, 4, 3));
	}		
	
}
