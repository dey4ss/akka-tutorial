package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.*;

public class CharSetManager {

    @Getter @AllArgsConstructor
    public static class CharSet {
        private final Set<Character> set;
        private final Character excludedChar;
    }

    private final Set<Character> originalSet;
    private List<CharSet> charSets;

    public static CharSetManager fromMessageLine(String[] line) {
        return new CharSetManager(line[2]);
    }

    private CharSetManager(String chars) {
        this.originalSet = parseChars(chars);
        this.charSets = generateSubsets(this.originalSet);
    }

    public boolean hasNext() {
        return this.charSets.size() > 0;
    }

    public CharSet next() {
        CharSet candidate = this.charSets.get(0);
        this.charSets.remove(0);
        return candidate;
    }

    public void reset() {
        this.charSets = generateSubsets(this.originalSet);
        Collections.reverse(charSets);
    }

    public static Set<Character> parseChars(String string) {
        Set<Character> set = new HashSet<>();
        for (int i = 0; i < string.length(); i++) {
            set.add(string.charAt(i));
        }
        return set;
    }

    private static List<CharSet> generateSubsets(Set<Character> chars) {
        List<CharSet> charSets = new LinkedList<>();
        for (Character c : chars) {
            Set<Character> set = new HashSet<>(chars);
            set.remove(c);
            charSets.add(new CharSet(set, c));
        }
        return charSets;
    }

}