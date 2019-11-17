package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter @AllArgsConstructor
public class Person {

	@Getter @AllArgsConstructor
	public static class CharSet {
		private final Set<Character> set;
		private final Character excludedChar;
	}

	private final Integer id;
	private final String name;
	private final int passwordLength;
	private final Set<Character> charSet;
	private final Set<Character> solutionSet = new HashSet<>();
	private final Set<Character> excludedChars = new HashSet<>();
	private final String passwordHash;
	private final List<String> hints;
	private final List<CharSet> candidateCharSets;
	private final int solutionSize;
	@Setter boolean cracked;

	public static Person fromList(String[] list) {
		Integer id = Integer.valueOf(list[0]);
		Set<Character> charSet = parseChars(list[2]);
		int length = Integer.parseInt(list[3]);
		List<String> hints = getHintHashes(list);
		List<CharSet> candidateSets = generateCandidates(charSet);
		int solutionSize = charSet.size() - hints.size();
		return new Person(
				id,
				list[1],
				length,
				charSet,
				list[4],
				hints,
				candidateSets,
				solutionSize,
				false);
	}

	public void addChar(char c) {
		this.solutionSet.add(c);
	}

	public void dropChar(char c) {
		this.excludedChars.add(c);
	}

	public boolean isReadyForCracking() {
		if (this.solutionSet.size() == this.solutionSize) {
			return true;
		}
		if (this.excludedChars.size() == this.charSet.size() - this.solutionSize) {
			this.solutionSet.addAll(this.charSet);
			this.solutionSet.removeAll(this.excludedChars);
			return true;
		}
		return false;
	}

	public boolean hasCharSetLeft() {
		return this.candidateCharSets.size() > 0;
	}

	public CharSet popCandidateCharSet() {
		if (this.candidateCharSets.size() > 0) {
			CharSet candidate = this.candidateCharSets.get(0);
			this.candidateCharSets.remove(0);
			return candidate;
		}
		return null;
	}

	private static Set<Character> parseChars(String string) {
		Set<Character> set = new HashSet<>();
		for (int i = 0; i < string.length(); i++) {
			set.add(string.charAt(i));
		}
		return set;
	}

	private static List<String> getHintHashes(String[] list) {
		return new ArrayList<>(Arrays.asList(list).subList(5, list.length));
	}

	private static List<CharSet> generateCandidates(Set<Character> set) {
		List<CharSet> candidates = new LinkedList<>();
		for (Character c : set) {
			Set<Character> candidate = new HashSet<>(set);
			candidate.remove(c);
			candidates.add(new CharSet(candidate, c));
		}
		return candidates;
	}

}
