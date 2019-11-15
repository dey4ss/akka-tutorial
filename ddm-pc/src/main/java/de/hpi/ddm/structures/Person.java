package de.hpi.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Getter @AllArgsConstructor
public class Person {

    private final Integer id;
    private final String name;
    private final int passwordLength;
    private final Set<Character> charSet;
    private final String passwordHash;
    private final List<String> hints;
    private final int finalCharSetSize;

    public static Person fromList(String[] list) {
        Integer id = Integer.valueOf(list[0]);
        Set<Character> charSet = parseChars(list[2]);
        int length = Integer.parseInt(list[3]);
        List<String> hints = getHintHashes(list);

        return new Person(id, list[1], length, charSet, list[4], hints, charSet.size() - hints.size());
    }

    public void dropChar(char c) {
        this.charSet.remove(c);
    }

    public boolean hasAllHints() {
        return charSet.size() == finalCharSetSize;
    }

    private static Set<Character> parseChars(String string) {
        Set<Character> set = new HashSet<>();
        for (int i = 0; i < string.length(); i++) {
            set.add(string.charAt(i));
        }
        return set;
    }

    private static List<String> getHintHashes(String[] list) {
        List<String> hints = new ArrayList<>();
        for (int i = 5; i < list.length; i++) {
            hints.add(list[i]);
        }
        return hints;
    }
}
